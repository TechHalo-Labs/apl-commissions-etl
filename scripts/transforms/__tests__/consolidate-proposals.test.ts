/**
 * Unit Tests for Proposal Consolidation Algorithm
 * 
 * Tests the iterative consolidation logic with various scenarios
 */

// Mock proposal interface matching the real one
interface TestProposal {
  Id: string;
  GroupId: string;
  SplitConfigurationMD5: string;
  DateRangeFrom: number;
  DateRangeTo: number | null;
  EffectiveDateFrom: Date;
  EffectiveDateTo: Date | null;
  ProductCodes: string;
  PlanCodes: string;
  [key: string]: any;
}

// Simplified test version of consolidateIteratively
function testConsolidateIteratively(proposals: TestProposal[]): { retained: TestProposal[]; consumed: Map<string, any> } {
  const retained: TestProposal[] = [];
  const consumed = new Map<string, any>();
  
  let currentGroup: string | null = null;
  let retainedProposal: TestProposal | null = null;
  const retainedProductCodes = new Set<string>();
  const retainedPlanCodes = new Set<string>();
  
  const parseCodesIntoSet = (jsonStr: string, targetSet: Set<string>): void => {
    if (jsonStr === '*') {
      targetSet.add('*');
      return;
    }
    try {
      const arr = JSON.parse(jsonStr);
      arr.forEach((code: string) => targetSet.add(code));
    } catch (e) {
      targetSet.add(jsonStr);
    }
  };
  
  const hasPlanConflict = (set1: Set<string>, set2: Set<string>): boolean => {
    if (set1.has('*') || set2.has('*')) return false;
    const intersection = new Set([...set1].filter(x => set2.has(x)));
    if (intersection.size === 0) return false;
    if (intersection.size === set1.size && intersection.size === set2.size) return false;
    return true;
  };
  
  for (const proposal of proposals) {
    // Rule 1: Different group
    if (currentGroup !== proposal.GroupId) {
      if (retainedProposal) {
        retained.push(retainedProposal);
      }
      currentGroup = proposal.GroupId;
      retainedProposal = { ...proposal };
      retainedProductCodes.clear();
      retainedPlanCodes.clear();
      parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
      parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
      continue;
    }
    
    // Rule 2: Different split config
    if (retainedProposal!.SplitConfigurationMD5 !== proposal.SplitConfigurationMD5) {
      retained.push(retainedProposal!);
      retainedProposal = { ...proposal };
      retainedProductCodes.clear();
      retainedPlanCodes.clear();
      parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
      parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
      continue;
    }
    
    // Rule 3: Conflicting plan codes
    const proposalPlanCodes = new Set<string>();
    parseCodesIntoSet(proposal.PlanCodes, proposalPlanCodes);
    
    if (hasPlanConflict(retainedPlanCodes, proposalPlanCodes)) {
      retained.push(retainedProposal!);
      retainedProposal = { ...proposal };
      retainedProductCodes.clear();
      retainedPlanCodes.clear();
      parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
      parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
      continue;
    }
    
    // Rule 4: Same config - extend dates and accumulate
    retainedProposal!.DateRangeFrom = Math.min(retainedProposal!.DateRangeFrom, proposal.DateRangeFrom);
    retainedProposal!.DateRangeTo = proposal.DateRangeTo === null 
      ? null 
      : retainedProposal!.DateRangeTo === null
        ? proposal.DateRangeTo
        : Math.max(retainedProposal!.DateRangeTo, proposal.DateRangeTo);
    
    parseCodesIntoSet(proposal.ProductCodes, retainedProductCodes);
    retainedProposal!.ProductCodes = JSON.stringify([...retainedProductCodes].sort());
    
    parseCodesIntoSet(proposal.PlanCodes, retainedPlanCodes);
    retainedProposal!.PlanCodes = JSON.stringify([...retainedPlanCodes].sort());
    
    consumed.set(proposal.Id, {
      proposalId: retainedProposal!.Id,
      reason: 'Same split configuration, extended date range'
    });
  }
  
  if (retainedProposal) {
    retained.push(retainedProposal);
  }
  
  return { retained, consumed };
}

// Test suite
describe('Proposal Consolidation', () => {
  test('should consolidate proposals with same config and contiguous dates', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2021, DateRangeTo: 2022,
        EffectiveDateFrom: new Date('2021-01-01'), EffectiveDateTo: new Date('2022-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);
    expect(result.retained[0].DateRangeFrom).toBe(2020);
    expect(result.retained[0].DateRangeTo).toBe(2022);
    expect(result.consumed.size).toBe(1);
    expect(result.consumed.get('P2')?.proposalId).toBe('P1');
  });
  
  test('should consolidate proposals with non-contiguous dates', () => {
    // User requirement: 2020-2021 + 2022-2023 = 2020-2023
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2022, DateRangeTo: 2023,
        EffectiveDateFrom: new Date('2022-01-01'), EffectiveDateTo: new Date('2023-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);
    expect(result.retained[0].DateRangeFrom).toBe(2020);
    expect(result.retained[0].DateRangeTo).toBe(2023);  // Gap filled
    expect(result.consumed.size).toBe(1);
  });
  
  test('should separate proposals with different split configs', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'XYZ',  // Different config
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(2);
    expect(result.consumed.size).toBe(0);
  });
  
  test('should separate proposals with conflicting plan codes', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC',  // Same config
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1","PLAN2"]'  // Partial overlap
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(2);  // Partial overlap = conflict
    expect(result.consumed.size).toBe(0);
  });
  
  test('should accumulate product codes when consolidating', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2021, DateRangeTo: 2022,
        EffectiveDateFrom: new Date('2021-01-01'), EffectiveDateTo: new Date('2022-12-31'),
        ProductCodes: '["VISION"]', PlanCodes: '["PLAN1"]'  // Different product, same plan
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);
    const products = JSON.parse(result.retained[0].ProductCodes);
    expect(products).toContain('DENTAL');
    expect(products).toContain('VISION');
    expect(result.consumed.size).toBe(1);
  });
  
  test('should reset when group changes', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: null,
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G2', SplitConfigurationMD5: 'ABC',  // Different group, same config
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: null,
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(2);  // Different groups, no consolidation
    expect(result.consumed.size).toBe(0);
  });
  
  test('should handle wildcard plan codes without conflict', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: null,
        ProductCodes: '["DENTAL"]', PlanCodes: '*'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2021, DateRangeTo: 2022,
        EffectiveDateFrom: new Date('2021-01-01'), EffectiveDateTo: null,
        ProductCodes: '["VISION"]', PlanCodes: '["PLAN1"]'  // Specific plan, but wildcard exists
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);  // Wildcard matches everything
    expect(result.consumed.size).toBe(1);
  });
});

// Run tests (simple implementation without jest)
console.log('Running consolidation tests...\n');

let passed = 0;
let failed = 0;

function expect(value: any) {
  return {
    toBe: (expected: any) => {
      if (value !== expected) {
        throw new Error(`Expected ${expected}, got ${value}`);
      }
    },
    toContain: (expected: any) => {
      if (!value.includes(expected)) {
        throw new Error(`Expected array to contain ${expected}, got ${value}`);
      }
    }
  };
}

function test(name: string, fn: () => void) {
  try {
    fn();
    console.log(`✅ ${name}`);
    passed++;
  } catch (error) {
    console.error(`❌ ${name}`);
    console.error(`   ${(error as Error).message}`);
    failed++;
  }
}

function describe(name: string, fn: () => void) {
  console.log(`\n${name}:`);
  fn();
}

// Run the tests
describe('Proposal Consolidation', () => {
  test('should consolidate proposals with same config and contiguous dates', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2021, DateRangeTo: 2022,
        EffectiveDateFrom: new Date('2021-01-01'), EffectiveDateTo: new Date('2022-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);
    expect(result.retained[0].DateRangeFrom).toBe(2020);
    expect(result.retained[0].DateRangeTo).toBe(2022);
    expect(result.consumed.size).toBe(1);
  });
  
  test('should consolidate proposals with non-contiguous dates', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2022, DateRangeTo: 2023,
        EffectiveDateFrom: new Date('2022-01-01'), EffectiveDateTo: new Date('2023-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);
    expect(result.retained[0].DateRangeFrom).toBe(2020);
    expect(result.retained[0].DateRangeTo).toBe(2023);
    expect(result.consumed.size).toBe(1);
  });
  
  test('should separate proposals with different split configs', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'XYZ', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(2);
    expect(result.consumed.size).toBe(0);
  });
  
  test('should separate proposals with conflicting plan codes', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1","PLAN2"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(2);
    expect(result.consumed.size).toBe(0);
  });
  
  test('should accumulate product codes', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: new Date('2021-12-31'),
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2021, DateRangeTo: 2022,
        EffectiveDateFrom: new Date('2021-01-01'), EffectiveDateTo: new Date('2022-12-31'),
        ProductCodes: '["VISION"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(1);
    const products = JSON.parse(result.retained[0].ProductCodes);
    expect(products).toContain('DENTAL');
    expect(products).toContain('VISION');
    expect(result.consumed.size).toBe(1);
  });
  
  test('should reset when group changes', () => {
    const proposals: TestProposal[] = [
      { 
        Id: 'P1', GroupId: 'G1', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: null,
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
      { 
        Id: 'P2', GroupId: 'G2', SplitConfigurationMD5: 'ABC', 
        DateRangeFrom: 2020, DateRangeTo: 2021,
        EffectiveDateFrom: new Date('2020-01-01'), EffectiveDateTo: null,
        ProductCodes: '["DENTAL"]', PlanCodes: '["PLAN1"]'
      },
    ];
    
    const result = testConsolidateIteratively(proposals);
    
    expect(result.retained.length).toBe(2);
    expect(result.consumed.size).toBe(0);
  });
});

console.log(`\n${'='.repeat(60)}`);
console.log(`Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(60));

if (failed > 0) {
  process.exit(1);
}
