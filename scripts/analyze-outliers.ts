/**
 * Outlier Detection Analysis
 * 
 * Identifies statistically significant outliers within groups where:
 * - A dominant SplitConfigHash exists (>90% of records)
 * - Minority configs are potential data anomalies
 * 
 * Usage:
 *   npx tsx scripts/analyze-outliers.ts [options]
 * 
 * Options:
 *   --groups G1234,G5678   Analyze specific groups
 *   --all                  Analyze all groups
 *   --threshold 0.05       Outlier threshold (default: 5% = 0.05)
 *   --min-dominant 0.90    Minimum for dominant config (default: 90%)
 *   --min-records 3        Minimum outlier records to flag (default: 3)
 *   --route-to-pha         Actually route outliers to PHA
 *   --dry-run              Show what would be done
 */

import * as sql from 'mssql';
import * as crypto from 'crypto';

interface OutlierResult {
  groupId: string;
  totalRecords: number;
  dominantConfig: {
    hash: string;
    count: number;
    percent: number;
    products: string[];
  } | null;
  outliers: {
    hash: string;
    count: number;
    percent: number;
    products: string[];
    planCodes: string[];
    sampleCertificates: string[];
  }[];
  isClean: boolean;
}

interface AnalysisOptions {
  outlierThreshold: number;      // Max % to be considered outlier
  minDominantPercent: number;    // Min % to be considered dominant
  minOutlierRecords: number;     // Min records to flag as outlier
  routeToPha: boolean;
  dryRun: boolean;
}

function parseConnectionString(connStr: string) {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: { encrypt: true, trustServerCertificate: true },
    requestTimeout: 300000,
  };
}

async function analyzeGroupOutliers(
  pool: sql.ConnectionPool,
  groupId: string,
  options: AnalysisOptions
): Promise<OutlierResult> {
  const groupIdNumeric = groupId.replace(/^[A-Za-z]+/, '');
  const groupIdWithPrefix = `G${groupIdNumeric}`;

  // Get config distribution for this group based on the split hierarchy
  // Group by the broker chain (SplitBrokerId sequence per certificate)
  const configQuery = await pool.request().query(`
    WITH CertHierarchy AS (
      SELECT 
        ci.CertificateId,
        ci.Product,
        ci.PlanCode,
        -- Create a signature from the broker hierarchy
        STRING_AGG(
          CONCAT(ISNULL(ci.SplitBrokerId,'NULL'), ':', ISNULL(ci.CommissionsSchedule,'NULL')),
          '|'
        ) AS HierarchySignature
      FROM [etl].[input_certificate_info] ci
      WHERE LTRIM(RTRIM(ci.GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
        AND ci.CertStatus = 'A'
        AND ci.RecStatus = 'A'
      GROUP BY ci.CertificateId, ci.Product, ci.PlanCode
    ),
    -- Count by hierarchy signature
    HierarchyCounts AS (
      SELECT 
        HierarchySignature,
        COUNT(DISTINCT CertificateId) AS CertCount,
        (SELECT STRING_AGG(p, ',') FROM (SELECT DISTINCT Product AS p FROM CertHierarchy ch2 WHERE ch2.HierarchySignature = ch.HierarchySignature) x) AS Products,
        (SELECT STRING_AGG(p, ',') FROM (SELECT DISTINCT PlanCode AS p FROM CertHierarchy ch2 WHERE ch2.HierarchySignature = ch.HierarchySignature) x) AS PlanCodes
      FROM CertHierarchy ch
      GROUP BY HierarchySignature
    )
    SELECT 
      CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', HierarchySignature), 2) AS ConfigHash,
      HierarchySignature AS ConfigSignature,
      CertCount,
      Products,
      PlanCodes,
      SUM(CertCount) OVER () AS TotalCerts,
      CAST(CertCount AS FLOAT) / NULLIF(CAST(SUM(CertCount) OVER () AS FLOAT), 0) * 100 AS Percentage
    FROM HierarchyCounts
    ORDER BY CertCount DESC
  `);

  const configs = configQuery.recordset;
  
  if (configs.length === 0) {
    return {
      groupId: groupIdWithPrefix,
      totalRecords: 0,
      dominantConfig: null,
      outliers: [],
      isClean: true
    };
  }

  const totalRecords = configs[0]?.TotalCerts || 0;
  
  // Find dominant config (first one since sorted by count desc)
  const topConfig = configs[0];
  const dominantConfig = topConfig.Percentage >= options.minDominantPercent * 100 ? {
    hash: topConfig.ConfigHash,
    count: topConfig.CertCount,
    percent: topConfig.Percentage,
    products: topConfig.Products?.split(',') || []
  } : null;

  // Find outliers (minority configs when there's a dominant one)
  const outliers: OutlierResult['outliers'] = [];
  
  if (dominantConfig) {
    for (let i = 1; i < configs.length; i++) {
      const cfg = configs[i];
      const pct = cfg.Percentage;
      
      // Is this an outlier? (below threshold AND has minimum records)
      if (pct <= options.outlierThreshold * 100 && cfg.CertCount >= options.minOutlierRecords) {
        // Get sample certificates for this config
        const sampleQuery = await pool.request().query(`
          SELECT TOP 5 CertificateId 
          FROM [etl].[input_certificate_info]
          WHERE LTRIM(RTRIM(GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
            AND CertStatus = 'A' AND RecStatus = 'A'
            AND Product IN (${cfg.Products.split(',').map((p: string) => `'${p.trim()}'`).join(',')})
        `);

        outliers.push({
          hash: cfg.ConfigHash,
          count: cfg.CertCount,
          percent: pct,
          products: cfg.Products?.split(',') || [],
          planCodes: cfg.PlanCodes?.split(',') || [],
          sampleCertificates: sampleQuery.recordset.map((r: any) => r.CertificateId)
        });
      }
    }
  }

  return {
    groupId: groupIdWithPrefix,
    totalRecords,
    dominantConfig,
    outliers,
    isClean: outliers.length === 0
  };
}

async function routeOutliersToPha(
  pool: sql.ConnectionPool,
  groupId: string,
  outliers: OutlierResult['outliers'],
  dryRun: boolean
): Promise<number> {
  if (outliers.length === 0) return 0;
  
  const groupIdNumeric = groupId.replace(/^[A-Za-z]+/, '');
  const groupIdWithPrefix = `G${groupIdNumeric}`;
  
  let totalRouted = 0;
  
  for (const outlier of outliers) {
    // Get all certificates for this outlier config
    const productsIn = outlier.products.map(p => `'${p.trim()}'`).join(',');
    
    const certsQuery = await pool.request().query(`
      SELECT DISTINCT CertificateId
      FROM [etl].[input_certificate_info]
      WHERE LTRIM(RTRIM(GroupId)) IN ('${groupIdNumeric}', '${groupIdWithPrefix}')
        AND CertStatus = 'A' AND RecStatus = 'A'
        AND Product IN (${productsIn})
    `);
    
    const certs = certsQuery.recordset;
    
    if (dryRun) {
      console.log(`    [DRY RUN] Would route ${certs.length} certs to PHA for outlier config`);
      totalRouted += certs.length;
      continue;
    }
    
    // Create PHA hierarchy for outliers
    const hierarchyId = `${groupIdWithPrefix}-PHA-OUTLIER-${outlier.hash.substring(0, 8)}`;
    const hierarchyVersionId = `${hierarchyId}-V1`;
    
    // Check if hierarchy exists
    const existingH = await pool.request().query(`
      SELECT Id FROM [etl].[stg_hierarchies] WHERE Id = '${hierarchyId}'
    `);
    
    if (existingH.recordset.length === 0) {
      await pool.request().query(`
        INSERT INTO [etl].[stg_hierarchies] (Id, Name, GroupId, Status, CurrentVersionId, SourceType, CreationTime, IsDeleted)
        VALUES ('${hierarchyId}', '${groupIdWithPrefix} Outlier Config PHA', '${groupIdWithPrefix}', 1, '${hierarchyVersionId}', 'Outlier-Detection', GETUTCDATE(), 0)
      `);
      
      await pool.request().query(`
        INSERT INTO [etl].[stg_hierarchy_versions] (Id, HierarchyId, Version, Status, EffectiveFrom, EffectiveTo, CreationTime, IsDeleted)
        VALUES ('${hierarchyVersionId}', '${hierarchyId}', 1, 1, '1900-01-01', '2099-12-31', GETUTCDATE(), 0)
      `);
    }
    
    // Insert PHA records
    for (const cert of certs) {
      const phaId = `PHA-OUTLIER-${cert.CertificateId}-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
      
      const existingPha = await pool.request().query(`
        SELECT 1 FROM [etl].[stg_policy_hierarchy_assignments]
        WHERE PolicyId = '${cert.CertificateId}' AND HierarchyId = '${hierarchyId}'
      `);
      
      if (existingPha.recordset.length === 0) {
        await pool.request().query(`
          INSERT INTO [etl].[stg_policy_hierarchy_assignments] (
            Id, PolicyId, HierarchyId, SplitSequence, SplitPercent,
            IsNonConforming, NonConformantReason, CreationTime, IsDeleted
          )
          VALUES (
            '${phaId}', '${cert.CertificateId}', '${hierarchyId}', 1, 100,
            1, 'Statistical outlier: ${outlier.percent.toFixed(1)}% of group (products: ${outlier.products.join(',')})', GETUTCDATE(), 0
          )
        `);
        totalRouted++;
      }
    }
  }
  
  return totalRouted;
}

async function main() {
  const args = process.argv.slice(2);
  
  const options: AnalysisOptions = {
    outlierThreshold: args.includes('--threshold')
      ? parseFloat(args[args.indexOf('--threshold') + 1])
      : 0.05,
    minDominantPercent: args.includes('--min-dominant')
      ? parseFloat(args[args.indexOf('--min-dominant') + 1])
      : 0.90,
    minOutlierRecords: args.includes('--min-records')
      ? parseInt(args[args.indexOf('--min-records') + 1], 10)
      : 3,
    routeToPha: args.includes('--route-to-pha'),
    dryRun: args.includes('--dry-run')
  };
  
  let groups: string[] = [];
  if (args.includes('--groups')) {
    const groupsArg = args[args.indexOf('--groups') + 1];
    groups = groupsArg.split(',').map(g => g.trim());
  }
  const analyzeAll = args.includes('--all');
  
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    throw new Error('SQLSERVER environment variable not set');
  }
  
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║  OUTLIER DETECTION ANALYSIS                                    ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`Settings:`);
  console.log(`  Outlier threshold: <${(options.outlierThreshold * 100).toFixed(1)}%`);
  console.log(`  Dominant threshold: >${(options.minDominantPercent * 100).toFixed(1)}%`);
  console.log(`  Min outlier records: ${options.minOutlierRecords}`);
  if (options.routeToPha) console.log(`  Mode: ROUTE TO PHA${options.dryRun ? ' (DRY RUN)' : ''}`);
  console.log('');
  
  const pool = await sql.connect(parseConnectionString(connStr));
  
  try {
    // Load groups if --all
    if (analyzeAll) {
      const result = await pool.request().query(`
        SELECT DISTINCT LTRIM(RTRIM(GroupId)) AS GroupId
        FROM [etl].[input_certificate_info]
        WHERE CertStatus = 'A' AND RecStatus = 'A'
        ORDER BY GroupId
      `);
      groups = result.recordset.map(r => r.GroupId).filter(g => g && g.trim());
      console.log(`Analyzing ${groups.length} groups...\n`);
    }
    
    if (groups.length === 0) {
      console.error('ERROR: Specify --groups or --all');
      process.exit(1);
    }
    
    const results: OutlierResult[] = [];
    const groupsWithOutliers: OutlierResult[] = [];
    
    for (let i = 0; i < groups.length; i++) {
      const group = groups[i];
      process.stdout.write(`  [${i + 1}/${groups.length}] Analyzing ${group}... `);
      
      const result = await analyzeGroupOutliers(pool, group, options);
      results.push(result);
      
      if (result.outliers.length > 0) {
        groupsWithOutliers.push(result);
        console.log(`⚠️  ${result.outliers.length} outlier config(s) found`);
        
        for (const outlier of result.outliers) {
          console.log(`    - ${outlier.percent.toFixed(2)}% (${outlier.count} certs): ${outlier.products.join(',')}`);
        }
        
        if (options.routeToPha) {
          const routed = await routeOutliersToPha(pool, group, result.outliers, options.dryRun);
          if (routed > 0) {
            console.log(`    ${options.dryRun ? '[DRY RUN] Would route' : 'Routed'} ${routed} certs to PHA`);
          }
        }
      } else if (result.dominantConfig) {
        console.log(`✓ clean (${result.dominantConfig.percent.toFixed(1)}% dominant)`);
      } else {
        console.log(`✓ no dominant pattern`);
      }
    }
    
    // Summary
    console.log('\n' + '═'.repeat(60));
    console.log('SUMMARY');
    console.log('═'.repeat(60));
    console.log(`Total groups analyzed: ${results.length}`);
    console.log(`Groups with outliers: ${groupsWithOutliers.length}`);
    
    if (groupsWithOutliers.length > 0) {
      console.log(`\nGroups requiring attention:`);
      for (const r of groupsWithOutliers) {
        const totalOutlierCerts = r.outliers.reduce((sum, o) => sum + o.count, 0);
        console.log(`  ${r.groupId}: ${r.outliers.length} outlier config(s), ${totalOutlierCerts} certs (${(totalOutlierCerts / r.totalRecords * 100).toFixed(2)}% of group)`);
      }
    }
    
  } finally {
    await pool.close();
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ Error:', err.message || err);
    process.exit(1);
  });
