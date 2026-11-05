const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Read app.js without changing it
const code = fs.readFileSync('./app.js', 'utf8');

// Provide minimal browser-like objects
const context = {
  window: {
    addEventListener: () => {}
  },
  document: {
    getElementById: () => null
  }
};

// Execute app.js in an isolated environment
vm.createContext(context);
vm.runInContext(code, context);

const calculateMonthlyTax = context.calculateMonthlyTax;

const cases = [
{ income: 0, expectedAnnualTax: 0 },
{ income: 15000, expectedAnnualTax: 0 },
{ income: 20000, expectedAnnualTax: 0 },
{ income: 25000, expectedAnnualTax: 100 },
{ income: 30000, expectedAnnualTax: 200 },
{ income: 35000, expectedAnnualTax: 375 },
{ income: 40000, expectedAnnualTax: 550 },
{ income: 60000, expectedAnnualTax: 1950 },
{ income: 80000, expectedAnnualTax: 3350 },
{ income: 100000, expectedAnnualTax: 5650 },
{ income: 120000, expectedAnnualTax: 7950 },
{ income: 150000, expectedAnnualTax: 12450 },
{ income: 160000, expectedAnnualTax: 13950 },
{ income: 180000, expectedAnnualTax: 17550 },
{ income: 200000, expectedAnnualTax: 21150 },
{ income: 220000, expectedAnnualTax: 24950 },
{ income: 240000, expectedAnnualTax: 28750 },
{ income: 260000, expectedAnnualTax: 32650 },
{ income: 280000, expectedAnnualTax: 36550 },
{ income: 300000, expectedAnnualTax: 40550 },
{ income: 320000, expectedAnnualTax: 44550 },
{ income: 400000, expectedAnnualTax: 62150 },
{ income: 500000, expectedAnnualTax: 84150 },
{ income: 750000, expectedAnnualTax: 141650 },
{ income: 1000000, expectedAnnualTax: 199150 },
{ income: 1200000, expectedAnnualTax: 247150 },
// boundary cases
  { income: 19999, expectedAnnualTax: 0 },
{ income: 20000, expectedAnnualTax: 0 },
{ income: 20001, expectedAnnualTax: 0.02 },
{ income: 79999, expectedAnnualTax: 3349.93 },
{ income: 80000, expectedAnnualTax: 3350 },
{ income: 80001, expectedAnnualTax: 3350.115 },
{ income: 119999, expectedAnnualTax: 7949.885 },
{ income: 120000, expectedAnnualTax: 7950 },
{ income: 120001, expectedAnnualTax: 7950.15 }

];
for (const { income, expectedAnnualTax } of cases) {
  test(`annual income $${income}`, () => {
    const actual = calculateMonthlyTax(income) * 12;
  assert.ok(
      Math.abs(actual - expectedAnnualTax) < 0.001,
      `Expected ${expectedAnnualTax}, got ${actual}`
    );
  });
}

const getEligibleDistricts = context.getEligibleDistricts;


// Rental filtering tests

test('districts without rental data for the selected bedroom count are excluded', () => {

  const data = [
    {
      district: 'D06',
      rentCondo: { '1': 4810 },
      commuteMetrics: { 'city_hall': 4 }
    },
    {
      district: 'D10',
      rentCondo: { '1': 3700, '5': 15950 },
      commuteMetrics: { 'city_hall': 14 }
    }
  ];

  const results = getEligibleDistricts(data, '5', 'city_hall');

  assert.equal(results.length, 1);

  assert.equal(results[0].district, 'D10');

});

test('districts with rental data for the selected bedroom count are shown', () => {

  const data = [
    {
      district: 'D06',
      rentCondo: { '1': 4810 },
      commuteMetrics: { 'city_hall': 4 }
    },
    {
      district: 'D10',
      rentCondo: { '1': 3700, '5': 15950 },
      commuteMetrics: { 'city_hall': 14 }
    }

  ];

  const results = getEligibleDistricts(data, '1', 'city_hall');

  assert.equal(results.length, 2);

});


test('districts without commute data for the selected office are excluded', () => {
  const data = [
    {
      district: 'D06',
      rentCondo: { '1': 4810 },
      commuteMetrics: { 'bugis': 4 }
    },
    {
      district: 'D10',
      rentCondo: { '1': 3700 },
      commuteMetrics: { 'city_hall': 14 }
    }
  ];

  const results = getEligibleDistricts(data, '1', 'city_hall');

  assert.equal(results.length, 1);
  assert.equal(results[0].district, 'D10');
});

test('districts with commute data for the selected office are shown', () => {
  const data = [
    {
      district: 'D06',
      rentCondo: { '1': 4810 },
      commuteMetrics: { 'city_hall': 4 }
    },
    {
      district: 'D10',
      rentCondo: { '1': 3700 },
      commuteMetrics: { 'city_hall': 14 }
    }
  ];

  const results = getEligibleDistricts(data, '1', 'city_hall');

  assert.equal(results.length, 2);
});