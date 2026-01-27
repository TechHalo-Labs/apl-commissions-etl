#!/usr/bin/env tsx
import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';

function parseConnectionString(connStr: string): Partial<sql.config> {
  const parts: Record<string, string> = {};
  connStr.split(';').forEach(part => {
    const [key, ...valueParts] = part.split('=');
    if (key && valueParts.length > 0) {
      parts[key.trim().toLowerCase()] = valueParts.join('=').trim();
    }
  });
  const encrypt = parts['encrypt'];
  const trustCert = parts['trustservercertificate'];
  return {
    server: parts['server'] || parts['data source'],
    database: parts['database'] || parts['initial catalog'],
    user: parts['user id'] || parts['uid'],
    password: parts['password'] || parts['pwd'],
    options: {
      encrypt: encrypt === undefined ? true : encrypt.toLowerCase() === 'true',
      trustServerCertificate: trustCert === undefined ? true : trustCert.toLowerCase() === 'true',
      enableArithAbort: true,
    },
  };
}

async function main() {
  const connStr = process.env.SQLSERVER;
  if (!connStr) {
    console.error('SQLSERVER environment variable not set');
    process.exit(1);
  }
  
  const config = parseConnectionString(connStr);
  const pool = await sql.connect(config as sql.config);
  
  const sqlPath = path.join(__dirname, 'diagnose-null-broker.sql');
  const sqlContent = fs.readFileSync(sqlPath, 'utf8');
  
  const result = await pool.request().query(sqlContent);
  
  // Print all recordsets
  for (let i = 0; i < result.recordsets.length; i++) {
    if (result.recordsets[i].length > 0) {
      console.log(`\nRecordset ${i + 1}:`);
      console.table(result.recordsets[i]);
    }
  }
  
  await pool.close();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
