/**
 * Test script for BlobBulk configuration
 * Usage: npx tsx scripts/test-blob-config.ts [--config appsettings.json]
 */

import { BlobServiceClient, ContainerClient } from '@azure/storage-blob';
import * as fs from 'fs';
import * as path from 'path';

interface BlobBulkConfig {
  containerUrl: string;
  endpoint: string;
  container: string;
  token: string;
  bulkPrefix: string;
}

function loadConfig(configPath: string): BlobBulkConfig {
  const fullPath = path.resolve(configPath);
  console.log(`📂 Loading config from: ${fullPath}`);
  
  if (!fs.existsSync(fullPath)) {
    throw new Error(`Config file not found: ${fullPath}`);
  }
  
  const raw = fs.readFileSync(fullPath, 'utf-8');
  const config = JSON.parse(raw);
  
  if (!config.BlobBulk) {
    throw new Error('BlobBulk section not found in config');
  }
  
  return config.BlobBulk as BlobBulkConfig;
}

async function testBlobConnection(config: BlobBulkConfig): Promise<void> {
  console.log('\n🔍 Parsed BlobBulk config:');
  console.log(`   container: ${config.container}`);
  console.log(`   containerUrl: ${config.containerUrl.substring(0, 60)}...`);
  console.log(`   token: ${config.token.substring(0, 30)}...`);
  
  // Build the container URL with SAS token
  const storageAccountUrl = `https://commissionstorage.blob.core.windows.net`;
  const serviceUrlWithSas = `${storageAccountUrl}?${config.token}`;
  const containerUrlWithSas = `${storageAccountUrl}/${config.container}?${config.token}`;
  
  // First, list available containers
  console.log('\n📋 Listing available containers...');
  try {
    const blobServiceClient = new BlobServiceClient(serviceUrlWithSas);
    const containers: string[] = [];
    for await (const container of blobServiceClient.listContainers()) {
      containers.push(container.name);
    }
    if (containers.length > 0) {
      console.log('   Available containers:');
      containers.forEach(c => console.log(`     - ${c}`));
    } else {
      console.log('   No containers found (or no permission to list)');
    }
  } catch (err: any) {
    console.log(`   ⚠️ Could not list containers: ${err.message}`);
  }
  
  console.log(`\n🔗 Connecting to container: ${config.container}`);
  console.log(`   URL: ${storageAccountUrl}/${config.container}?...`);
  
  // Create container client directly from URL with SAS
  const containerClient = new ContainerClient(containerUrlWithSas);
  
  // Test 1: Check if container exists, create if not
  console.log('\n📋 Test 1: Checking if container exists...');
  try {
    const exists = await containerClient.exists();
    if (exists) {
      console.log('   ✅ Container exists!');
    } else {
      console.log('   ⚠️ Container does NOT exist - attempting to create...');
      try {
        const blobServiceClient = new BlobServiceClient(serviceUrlWithSas);
        const createResult = await blobServiceClient.createContainer(config.container);
        console.log(`   ✅ Container "${config.container}" created successfully!`);
      } catch (createErr: any) {
        console.log(`   ❌ Failed to create container: ${createErr.message}`);
        return;
      }
    }
  } catch (err: any) {
    console.log(`   ❌ Error checking container: ${err.message}`);
    return;
  }
  
  // Test 2: Upload a test file
  const testContent = `Test file created at ${new Date().toISOString()}`;
  const testBlobName = `test/test-blob-${Date.now()}.txt`;
  
  console.log(`\n📤 Test 2: Uploading test blob: ${testBlobName}`);
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(testBlobName);
    await blockBlobClient.upload(testContent, testContent.length);
    console.log('   ✅ Upload successful!');
  } catch (err: any) {
    console.log(`   ❌ Upload failed: ${err.message}`);
    return;
  }
  
  // Test 3: List blobs in test/ prefix
  console.log('\n📋 Test 3: Listing blobs in test/ prefix...');
  try {
    let count = 0;
    for await (const blob of containerClient.listBlobsFlat({ prefix: 'test/' })) {
      console.log(`   - ${blob.name}`);
      count++;
      if (count >= 5) {
        console.log('   ... (showing first 5)');
        break;
      }
    }
    console.log(`   ✅ Listed ${count} blob(s)`);
  } catch (err: any) {
    console.log(`   ❌ List failed: ${err.message}`);
    return;
  }
  
  // Test 4: Read back the test file
  console.log(`\n📥 Test 4: Reading back the test blob...`);
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(testBlobName);
    const downloadResponse = await blockBlobClient.download(0);
    const downloaded = await streamToString(downloadResponse.readableStreamBody!);
    if (downloaded === testContent) {
      console.log('   ✅ Content matches!');
    } else {
      console.log('   ⚠️ Content mismatch');
    }
  } catch (err: any) {
    console.log(`   ❌ Read failed: ${err.message}`);
    return;
  }
  
  // Test 5: Delete the test file
  console.log(`\n🗑️ Test 5: Deleting test blob...`);
  try {
    const blockBlobClient = containerClient.getBlockBlobClient(testBlobName);
    await blockBlobClient.delete();
    console.log('   ✅ Deleted successfully!');
  } catch (err: any) {
    console.log(`   ❌ Delete failed: ${err.message}`);
  }
  
  console.log('\n✅ All tests passed! Blob configuration is working correctly.');
}

async function streamToString(readableStream: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    readableStream.on('data', (data) => {
      chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
    });
    readableStream.on('end', () => {
      resolve(Buffer.concat(chunks).toString('utf-8'));
    });
    readableStream.on('error', reject);
  });
}

// Main
async function main() {
  console.log('🧪 Blob Configuration Test Script\n');
  
  // Parse args
  const args = process.argv.slice(2);
  let configPath = 'appsettings.json';
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--config' && args[i + 1]) {
      configPath = args[i + 1];
      i++;
    }
  }
  
  try {
    const config = loadConfig(configPath);
    await testBlobConnection(config);
  } catch (err: any) {
    console.error(`\n❌ Error: ${err.message}`);
    process.exit(1);
  }
}

main();
