// Active State Machine Global Parameters
let activePropertyType = 'condo';
let activeSortPriority = 'least_commute';

window.toggleSortMenu = function() {
  const menu = document.getElementById("sort-dropdown-menu");
  if (menu) menu.classList.toggle("dropdown-active");
};

// FIXED EXECUTION: Force clear configuration caches and trigger a full mathematical data compilation on click

window.setSortPriority = function(mode) {
  activeSortPriority = mode;
  
  const optCommute = document.getElementById("opt-commute");
  const optCash = document.getElementById("opt-cash");
  
  if (optCommute) optCommute.classList.toggle("option-active", mode === 'least_commute');
  if (optCash) optCash.classList.toggle("option-active", mode === 'max_cash');
  
  const menu = document.getElementById("sort-dropdown-menu");
  if (menu) menu.classList.remove("dropdown-active");
  
  // Pipeline Refresher
  renderDashboard();
};

window.switchProperty = function(type) {
  activePropertyType = type;
  const toggleCondo = document.getElementById("toggle-condo");
  const toggleHdb = document.getElementById("toggle-hdb");
  if (toggleCondo) toggleCondo.classList.toggle("toggle-active", type === 'condo');
  if (toggleHdb) toggleHdb.classList.toggle("toggle-active", type === 'hdb');
  renderDashboard();
};

window.openHousingDrawer = function(e) {
  if (e) e.stopPropagation();
  document.body.classList.add("drawer-open");
};

window.closeHousingDrawer = function() {
  document.body.classList.remove("drawer-open");
};

window.addEventListener('click', function(e) {
  const container = e.target.closest('.sort-icon-container');
  const menu = document.getElementById("sort-dropdown-menu");
  if (!container && menu) {
    menu.classList.remove("dropdown-active");
  }

  const drawerPane = e.target.closest('.drawer-pane');
  const infoBtn = e.target.closest('.info-btn-trigger');
  if (!drawerPane && !infoBtn) {
    closeHousingDrawer();
  }

});

function getEligibleDistricts(data, selectedBeds, selectedHub) {

  return data.filter(item => {

    const rentMap = activePropertyType === 'condo'
      ? item.rentCondo
      : item.rentHdb;
    return rentMap?.[selectedBeds] != null && item.commuteMetrics?.[selectedHub] != null;

  });

}

function calculateMonthlyTax(annualGross) {

  const brackets = [

    [20000, 0],

    [30000, 0.02],

    [40000, 0.035],

    [80000, 0.07],

    [120000, 0.115],

    [160000, 0.15],

    [200000, 0.18],

    [240000, 0.19],

    [280000, 0.195],

    [320000, 0.20],

    [500000, 0.22],

    [1000000, 0.23],

    [Infinity, 0.24]

  ];

  let tax = 0;

  let previousLimit = 0;

  for (const [limit, rate] of brackets) {

    const taxableAmount = Math.min(

      Math.max(annualGross - previousLimit, 0),

      limit - previousLimit

    );

    tax += taxableAmount * rate;

    previousLimit = limit;

    if (annualGross <= limit) break;

  }

  return tax / 12;

}

function renderDashboard() {
  const salaryBox = document.getElementById("salary-box");
  const hubInput = document.getElementById("hub-input");
  const bedsInput = document.getElementById("beds-input");
  const container = document.getElementById("cards-container");
  
  if (!salaryBox || !hubInput || !bedsInput || !container) return;

  const salaryInput = parseInt(salaryBox.value) || 0;
  const selectedHub = hubInput.value;
  const selectedBeds = bedsInput.value; 
  
  const monthlyTax = calculateMonthlyTax(salaryInput * 12);
  const monthlyTransitCost = 45; 

  // STEP 1: Always build the COMPLETE full results set before running the sort routine [4.2]
let computedResults = getEligibleDistricts(

  NEIGHBORHOOD_DATA,

  selectedBeds,

  selectedHub

).map(item => {

  const commuteTime = item.commuteMetrics[selectedHub];

  const rentMap = activePropertyType === 'condo'

    ? item.rentCondo

    : item.rentHdb;

  const activeRent = rentMap[selectedBeds];

  const trueLeftover =

    salaryInput - monthlyTax - activeRent - monthlyTransitCost;

  return { ...item, activeRent, commuteTime, trueLeftover };

});

  // Standardized, error-proof arithmetic sorting engine router logic blocks [4.2]
  computedResults.sort((a, b) => {
    if (activeSortPriority === 'least_commute') {
      // Default Mode: Sort by commute time ascending. If equal, sort by trueLeftover descending [4.2].
      return a.commuteTime - b.commuteTime || b.trueLeftover - a.trueLeftover;
    } else {
      // Max Cash Mode: Sort by trueLeftover descending. If equal, sort by commute time ascending [4.2].
      return b.trueLeftover - a.trueLeftover || a.commuteTime - b.commuteTime;
    }
  });

  // STEP 3: Clear the canvas and render the re-ranked cohort layout
  container.innerHTML = "";

  computedResults.forEach((item) => {
    const cardHtml = `
      <div class="card" style="margin-bottom: 16px;">
        <h3>${item.district}</h3>
        <p class="commute-info">🚆 ${item.commuteTime} mins estimated commute via MRT</p>
        
        <div class="card-details">
          <div><span>Est. Selected Rent:</span> <strong>$${item.activeRent}/mo</strong></div>
          <div><span>Est. Tax Reserve:</span> <strong>$${Math.round(monthlyTax)}/mo</strong></div>
        </div>

        <div class="card-total">
          <span class="label">True Take-Home Leftover:</span>
          <span class="value ${item.trueLeftover < 0 ? 'text-red' : ''}">
            $${Math.round(item.trueLeftover).toLocaleString()}
          </span>
        </div>
      </div>
    `;
    container.innerHTML += cardHtml;
  });
}

function initSyncListeners() {
  const slider = document.getElementById("salary-input");
  const numberBox = document.getElementById("salary-box");
  const hubInput = document.getElementById("hub-input");
  const bedsInput = document.getElementById("beds-input");

  if (slider && numberBox) {
    slider.addEventListener("input", function() {
      numberBox.value = this.value;
      renderDashboard();
    });

    numberBox.addEventListener("input", function() {
      let val = parseInt(this.value) || 0;
      if (val >= 5000 && val <= 25000) {
        slider.value = val;
      }
      renderDashboard();
    });
  }

  if (hubInput) hubInput.addEventListener("change", renderDashboard);
  if (bedsInput) bedsInput.addEventListener("change", renderDashboard);
}

// Fire the setup routine on file load execution frame
initSyncListeners();
renderDashboard();
