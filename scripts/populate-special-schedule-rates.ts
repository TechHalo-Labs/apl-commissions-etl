#!/usr/bin/env tsx

import { ConnectionPool, Request, TYPES } from 'mssql';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

interface RawScheduleRate {
    ScheduleName: string;
    ProductCode: string;
    State: string;
    GroupSizeFrom: number;
    GroupSizeTo: number;
    Year1: number;
    Year2: number;
    Year3: number;
    Year4: number;
    Year5: number;
    Year6: number;
    Year7: number;
    Year8: number;
    Year9: number;
    Year10: number;
    Year11: number;
    Year12: number;
    Year13: number;
    Year14: number;
    Year15: number;
    Year16: number;
}

interface SpecialScheduleRate {
    ScheduleRateId: number;
    Year: number;
    Rate: number;
}

class SpecialScheduleRatesBuilder {
    private pool: ConnectionPool;

    constructor() {
        this.pool = new ConnectionPool({
            server: process.env.SQL_SERVER || 'halo-sql.database.windows.net',
            database: process.env.SQL_DATABASE || 'halo-sqldb',
            user: process.env.SQL_USERNAME || 'azadmin',
            password: process.env.SQL_PASSWORD || '',
            options: {
                encrypt: true,
                trustServerCertificate: false
            }
        });
    }

    async connect(): Promise<void> {
        await this.pool.connect();
        console.log('✅ Connected to SQL Server');
    }

    async disconnect(): Promise<void> {
        await this.pool.close();
        console.log('✅ Disconnected from SQL Server');
    }

    /**
     * Check if SpecialScheduleRates table exists
     */
    async checkTableExists(): Promise<boolean> {
        const result = await this.pool.request().query(`
            SELECT COUNT(*) as Count
            FROM sys.tables
            WHERE schema_id = SCHEMA_ID('dbo')
            AND name = 'SpecialScheduleRates'
        `);
        return result.recordset[0].Count > 0;
    }

    /**
     * Get schedules that have varying rates across years (with LIMIT for testing)
     */
    async getSchedulesWithVaryingRates(limit?: number): Promise<RawScheduleRate[]> {
        console.log('🔍 Finding schedules with varying rates...');

        const limitClause = limit ? `TOP ${limit}` : '';
        const result = await this.pool.request().query(`
            SELECT ${limitClause}
                rsr.ScheduleName,
                rsr.ProductCode,
                rsr.State,
                rsr.GroupSizeFrom,
                rsr.GroupSizeTo,
                rsr.Year1, rsr.Year2, rsr.Year3, rsr.Year4, rsr.Year5, rsr.Year6,
                rsr.Year7, rsr.Year8, rsr.Year9, rsr.Year10, rsr.Year11, rsr.Year12,
                rsr.Year13, rsr.Year14, rsr.Year15, rsr.Year16
            FROM old_etl.raw_schedule_rates rsr
            WHERE rsr.Year1 <> rsr.Year2 OR rsr.Year1 <> rsr.Year3 OR rsr.Year1 <> rsr.Year4 OR rsr.Year1 <> rsr.Year5 OR
                  rsr.Year1 <> rsr.Year6 OR rsr.Year1 <> rsr.Year7 OR rsr.Year1 <> rsr.Year8 OR rsr.Year1 <> rsr.Year9 OR
                  rsr.Year1 <> rsr.Year10 OR rsr.Year1 <> rsr.Year11 OR rsr.Year1 <> rsr.Year12 OR rsr.Year1 <> rsr.Year13 OR
                  rsr.Year1 <> rsr.Year14 OR rsr.Year1 <> rsr.Year15 OR rsr.Year1 <> rsr.Year16
        `);

        console.log(`📊 Found ${result.recordset.length} schedules with varying rates${limit ? ` (limited to ${limit})` : ''}`);
        return result.recordset as RawScheduleRate[];
    }

    /**
     * Transform raw schedule rates into individual year records
     */
    transformToYearRecords(schedules: RawScheduleRate[]): Array<{
        schedule: RawScheduleRate;
        year: number;
        rate: number;
    }> {
        const yearRecords: Array<{
            schedule: RawScheduleRate;
            year: number;
            rate: number;
        }> = [];

        for (const schedule of schedules) {
            // Extract each year's rate
            for (let year = 1; year <= 16; year++) {
                const rate = schedule[`Year${year}` as keyof RawScheduleRate] as number;
                yearRecords.push({
                    schedule,
                    year,
                    rate
                });
            }
        }

        console.log(`🔄 Transformed into ${yearRecords.length} year-by-year records`);
        return yearRecords;
    }

    /**
     * Get matching ScheduleRate IDs for the raw schedule records
     */
    async getScheduleRateMappings(yearRecords: Array<{
        schedule: RawScheduleRate;
        year: number;
        rate: number;
    }>): Promise<Map<string, number[]>> {
        console.log('🔗 Finding ScheduleRate mappings...');

        // Create a set of unique schedule combinations to query
        const uniqueSchedules = new Set<string>();
        for (const record of yearRecords) {
            const key = `${record.schedule.ProductCode}|${record.schedule.State}|${record.schedule.GroupSizeFrom}|${record.schedule.GroupSizeTo}`;
            uniqueSchedules.add(key);
        }

        const mappings = new Map<string, number[]>();

        // Query in batches to avoid parameter limits
        const scheduleArray = Array.from(uniqueSchedules);
        const batchSize = 100;

        for (let i = 0; i < scheduleArray.length; i += batchSize) {
            const batch = scheduleArray.slice(i, i + batchSize);

            const inClause = batch.map((_, idx) => `@p${idx}`).join(',');
            const request = this.pool.request();

            batch.forEach((key, idx) => {
                const [productCode, state, groupSizeFrom, groupSizeTo] = key.split('|');
                request.input(`p${idx}_pc`, TYPES.NVarChar, productCode);
                request.input(`p${idx}_st`, TYPES.NVarChar, state);
                request.input(`p${idx}_gf`, TYPES.Int, parseInt(groupSizeFrom));
                request.input(`p${idx}_gt`, TYPES.Int, parseInt(groupSizeTo));
            });

            // Build the WHERE clause dynamically
            const whereConditions = batch.map((_, idx) =>
                `(ProductCode = @p${idx}_pc AND State = @p${idx}_st AND GroupSizeFrom = @p${idx}_gf AND GroupSizeTo = @p${idx}_gt)`
            ).join(' OR ');

            const result = await request.query(`
                SELECT Id, ProductCode, State, GroupSizeFrom, GroupSizeTo
                FROM dbo.ScheduleRates
                WHERE ${whereConditions}
            `);

            // Store mappings
            for (const row of result.recordset) {
                const key = `${row.ProductCode}|${row.State}|${row.GroupSizeFrom}|${row.GroupSizeTo}`;
                if (!mappings.has(key)) {
                    mappings.set(key, []);
                }
                mappings.get(key)!.push(row.Id);
            }
        }

        console.log(`🔗 Found mappings for ${mappings.size} unique schedule combinations`);
        return mappings;
    }

    /**
     * Create the final records to insert
     */
    createInsertRecords(
        yearRecords: Array<{
            schedule: RawScheduleRate;
            year: number;
            rate: number;
        }>,
        mappings: Map<string, number[]>
    ): SpecialScheduleRate[] {
        const insertRecords: SpecialScheduleRate[] = [];

        for (const record of yearRecords) {
            const key = `${record.schedule.ProductCode}|${record.schedule.State}|${record.schedule.GroupSizeFrom}|${record.schedule.GroupSizeTo}`;
            const scheduleRateIds = mappings.get(key);

            if (scheduleRateIds) {
                for (const scheduleRateId of scheduleRateIds) {
                    insertRecords.push({
                        ScheduleRateId: scheduleRateId,
                        Year: record.year,
                        Rate: record.rate
                    });
                }
            }
        }

        console.log(`📝 Created ${insertRecords.length} final records for insertion`);
        return insertRecords;
    }

    /**
     * Check for existing records to avoid duplicates
     */
    async filterExistingRecords(records: SpecialScheduleRate[], dryRun: boolean = false): Promise<SpecialScheduleRate[]> {
        if (dryRun) {
            console.log('🔍 DRY RUN: Skipping duplicate check for performance');
            return records;
        }

        console.log('🔍 Checking for existing records to avoid duplicates...');

        // Use a more efficient approach: check in smaller batches and use EXISTS queries
        const existingKeys = new Set<string>();
        const batchSize = 500; // Smaller batch size for reliability

        for (let i = 0; i < records.length; i += batchSize) {
            const batch = records.slice(i, i + batchSize);

            // Create a temporary table for this batch to check existence efficiently
            const tempTableName = `#temp_batch_${Date.now()}_${i}`;

            try {
                // Create temp table with the batch data
                const createTempTable = `
                    CREATE TABLE ${tempTableName} (
                        ScheduleRateId BIGINT,
                        Year INT,
                        PRIMARY KEY (ScheduleRateId, Year)
                    )`;

                await this.pool.request().query(createTempTable);

                // Insert batch data into temp table
                const insertValues = batch.map((_, idx) =>
                    `(@sr${idx}, @y${idx})`
                ).join(',');

                const insertRequest = this.pool.request();
                batch.forEach((record, idx) => {
                    insertRequest.input(`sr${idx}`, TYPES.BigInt, record.ScheduleRateId);
                    insertRequest.input(`y${idx}`, TYPES.Int, record.Year);
                });

                await insertRequest.query(`
                    INSERT INTO ${tempTableName} (ScheduleRateId, Year)
                    VALUES ${insertValues}
                `);

                // Find existing records
                const result = await this.pool.request().query(`
                    SELECT tb.ScheduleRateId, tb.Year
                    FROM ${tempTableName} tb
                    INNER JOIN dbo.SpecialScheduleRates ssr ON
                        ssr.ScheduleRateId = tb.ScheduleRateId AND
                        ssr.Year = tb.Year
                `);

                for (const row of result.recordset) {
                    existingKeys.add(`${row.ScheduleRateId}-${row.Year}`);
                }

            } catch (error) {
                console.log(`⚠️ Batch duplicate check failed, using individual checks for batch ${Math.floor(i / batchSize) + 1}...`);
                // Fallback to individual checks for this batch
                for (const record of batch) {
                    try {
                        const result = await this.pool.request()
                            .input('scheduleRateId', TYPES.BigInt, record.ScheduleRateId)
                            .input('year', TYPES.Int, record.Year)
                            .query(`
                                SELECT COUNT(*) as Count
                                FROM dbo.SpecialScheduleRates
                                WHERE ScheduleRateId = @scheduleRateId AND Year = @year
                            `);

                        if (result.recordset[0].Count > 0) {
                            existingKeys.add(`${record.ScheduleRateId}-${record.Year}`);
                        }
                    } catch (individualError) {
                        console.log(`❌ Failed to check individual record: ${record.ScheduleRateId}-${record.Year}`);
                    }
                }
            } finally {
                // Clean up temp table
                try {
                    await this.pool.request().query(`DROP TABLE IF EXISTS ${tempTableName}`);
                } catch (cleanupError) {
                    // Ignore cleanup errors
                }
            }

            if ((i + batchSize) % 5000 === 0) {
                console.log(`  → Processed ${Math.min(i + batchSize, records.length)}/${records.length} records for duplicates...`);
            }
        }

        // Filter out existing records
        const newRecords = records.filter(record =>
            !existingKeys.has(`${record.ScheduleRateId}-${record.Year}`)
        );

        console.log(`🆕 Found ${newRecords.length} new records to insert (${records.length - newRecords.length} duplicates skipped)`);
        return newRecords;
    }

    /**
     * Insert records in batches
     */
    async insertRecords(records: SpecialScheduleRate[], dryRun: boolean = false): Promise<void> {
        if (records.length === 0) {
            console.log('ℹ️ No records to insert');
            return;
        }

        if (dryRun) {
            console.log(`🔍 DRY RUN: Would insert ${records.length} records`);
            console.log('📋 Sample records:');
            for (let i = 0; i < Math.min(5, records.length); i++) {
                console.log(`  - ScheduleRateId: ${records[i].ScheduleRateId}, Year: ${records[i].Year}, Rate: ${records[i].Rate}`);
            }
            return;
        }

        console.log(`📥 Inserting ${records.length} records in batches...`);

        const batchSize = 1000;
        let inserted = 0;

        for (let i = 0; i < records.length; i += batchSize) {
            const batch = records.slice(i, i + batchSize);
            const values = batch.map((_, idx) =>
                `(@sr${idx}, @y${idx}, @r${idx}, GETUTCDATE(), 0)`
            ).join(',');

            const request = this.pool.request();
            batch.forEach((record, idx) => {
                request.input(`sr${idx}`, TYPES.BigInt, record.ScheduleRateId);
                request.input(`y${idx}`, TYPES.Int, record.Year);
                request.input(`r${idx}`, TYPES.Decimal, record.Rate);
            });

            await request.query(`
                INSERT INTO dbo.SpecialScheduleRates (
                    ScheduleRateId, Year, Rate, CreationTime, IsDeleted
                )
                VALUES ${values}
            `);

            inserted += batch.length;
            console.log(`  → Inserted batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(records.length / batchSize)} (${inserted}/${records.length})`);
        }

        console.log(`✅ Successfully inserted ${inserted} records`);
    }

    /**
     * Main execution method
     */
    async build(dryRun: boolean = false, limit?: number): Promise<void> {
        try {
            console.log('🚀 Starting SpecialScheduleRates build process...');

            // Check if table exists
            const tableExists = await this.checkTableExists();
            if (!tableExists) {
                throw new Error('SpecialScheduleRates table does not exist. Please run database migrations first.');
            }

            // Get schedules with varying rates
            const varyingSchedules = await this.getSchedulesWithVaryingRates(limit);

            if (varyingSchedules.length === 0) {
                console.log('ℹ️ No schedules with varying rates found');
                return;
            }

            // Transform to year-by-year records
            const yearRecords = this.transformToYearRecords(varyingSchedules);

            // Get ScheduleRate mappings
            const mappings = await this.getScheduleRateMappings(yearRecords);

            // Create final records
            const insertRecords = this.createInsertRecords(yearRecords, mappings);

            // Filter out existing records
            const newRecords = await this.filterExistingRecords(insertRecords, dryRun);

            // Insert records
            await this.insertRecords(newRecords, dryRun);

            console.log('✅ SpecialScheduleRates build process completed successfully!');

        } catch (error) {
            console.error('❌ Error during build process:', error);
            throw error;
        }
    }
}

// CLI interface
async function main() {
    const args = process.argv.slice(2);
    const dryRun = args.includes('--dry-run') || args.includes('-d');

    // Parse limit parameter
    let limit: number | undefined;
    const limitIndex = args.findIndex(arg => arg.startsWith('--limit='));
    if (limitIndex !== -1) {
        limit = parseInt(args[limitIndex].split('=')[1]);
        if (isNaN(limit)) {
            console.error('❌ Invalid limit value. Use --limit=N where N is a number.');
            process.exit(1);
        }
    }

    const builder = new SpecialScheduleRatesBuilder();

    try {
        await builder.connect();

        if (dryRun) {
            console.log('🔍 DRY RUN MODE - No changes will be made to the database');
        }

        if (limit) {
            console.log(`📏 LIMIT MODE: Processing only ${limit} schedules`);
        }

        await builder.build(dryRun, limit);

    } catch (error) {
        console.error('❌ Build failed:', error);
        process.exit(1);
    } finally {
        await builder.disconnect();
    }
}

// Run if called directly
if (require.main === module) {
    main().catch(console.error);
}

export { SpecialScheduleRatesBuilder };