// High-Precision Real Estate Matrix - Fully Expanded 2026 Medians
// const NEIGHBORHOOD_DATA = [
//   {
//     district: "District 01 & 02 (Downtown Core / Tanjong Pagar)",
//     rentCondo: { "1": 4500, "2": 6200, "3": 8500, "4": 12500, "5": 16500 },
//     rentHdb:   { "1": 2600, "2": 3600, "3": 4400, "4": 4900,  "5": 5500 },
//     commuteMetrics: {
//       "raffles_place": 5, "tanjong_pagar": 5, "downtown": 5, "shenton_way": 5,
//       "one_north": 20, "buona_vista": 18, "bugis": 10, "city_hall": 8,
//       "harbourfront": 12, "paya_lebar": 16, "expo": 32, "jurong_east": 28
//     }
//   },
//   {
//     district: "District 03 (Queenstown / Tiong Bahru)",
//     rentCondo: { "1": 3900, "2": 5100, "3": 6800, "4": 9800,  "5": 13000 },
//     rentHdb:   { "1": 2200, "2": 3100, "3": 3800, "4": 4300,  "5": 4800 },
//     commuteMetrics: {
//       "raffles_place": 12, "tanjong_pagar": 10, "downtown": 16, "shenton_way": 14,
//       "one_north": 11, "buona_vista": 8, "bugis": 16, "city_hall": 14,
//       "harbourfront": 14, "paya_lebar": 24, "expo": 42, "jurong_east": 18
//     }
//   },
//   {
//     district: "District 15 (East Coast / Marine Parade)",
//     rentCondo: { "1": 3330, "2": 4500, "3": 5900, "4": 8400,  "5": 11000 },
//     rentHdb:   { "1": 2100, "2": 2800, "3": 3500, "4": 4000,  "5": 4500 },
//     commuteMetrics: {
//       "raffles_place": 22, "tanjong_pagar": 25, "downtown": 18, "shenton_way": 16,
//       "one_north": 38, "buona_vista": 36, "bugis": 20, "city_hall": 22,
//       "harbourfront": 26, "paya_lebar": 15, "expo": 25, "jurong_east": 46
//     }
//   },
//   {
//     district: "District 14 (Paya Lebar / Geylang)",
//     rentCondo: { "1": 3250, "2": 4300, "3": 5500, "4": 7800,  "5": 10500 },
//     rentHdb:   { "1": 2000, "2": 2700, "3": 3400, "4": 3900,  "5": 4400 },
//     commuteMetrics: {
//       "raffles_place": 14, "tanjong_pagar": 16, "downtown": 22, "shenton_way": 24,
//       "one_north": 29, "buona_vista": 27, "bugis": 10, "city_hall": 12,
//       "harbourfront": 26, "paya_lebar": 4, "expo": 20, "jurong_east": 34
//     }
//   },
//   {
//     district: "District 18 (Tampines / Pasir Ris)",
//     rentCondo: { "1": 2620, "2": 3600, "3": 4600, "4": 6200,  "5": 7900 },
//     rentHdb:   { "1": 1800, "2": 2400, "3": 3100, "4": 3500,  "5": 4000 },
//     commuteMetrics: {
//       "raffles_place": 34, "tanjong_pagar": 36, "downtown": 42, "shenton_way": 45,
//       "one_north": 54, "buona_vista": 52, "bugis": 30, "city_hall": 32,
//       "harbourfront": 48, "paya_lebar": 20, "expo": 12, "jurong_east": 54
//     }
//   }
// ];




// Private Condo

// Median monthly rentCondo: Q2 2026 (Apr-Jun)

// Minimum 10 transactions per district × bedroom combination

// Commute metrics: provided separately

const NEIGHBORHOOD_DATA = [

  {

    district: "D01 (Raffles Place / Marina Bay)",

    rentCondo: { "1": 4500, "2": 6500, "3": 9200, "4": 14000 },

    commuteMetrics: {

      "raffles_place": 5, "tanjong_pagar": 5, "downtown": 5, "shenton_way": 5,

      "one_north": 20, "buona_vista": 18, "bugis": 10, "city_hall": 8,

      "harbourfront": 12, "paya_lebar": 16, "expo": 32, "jurong_east": 28

    }

  },

  {

    district: "D02 (Tanjong Pagar / Anson)",

    rentCondo: { "1": 4000, "2": 5300, "3": 8050 },

    commuteMetrics: {

      "raffles_place": 5, "tanjong_pagar": 5, "downtown": 5, "shenton_way": 5,
      "one_north": 20, "buona_vista": 18, "bugis": 10, "city_hall": 8,

      "harbourfront": 12, "paya_lebar": 16, "expo": 32, "jurong_east": 28

    }

  },

  {

    district: "D03 (Queenstown / Tiong Bahru / Alexandra)",

    rentCondo: { "1": 3600, "2": 4800, "3": 6700, "4": 8500 },

    commuteMetrics: {

      "raffles_place": 12, "tanjong_pagar": 10, "downtown": 16, "shenton_way": 14,

      "one_north": 11, "buona_vista": 8, "bugis": 16, "city_hall": 14,

      "harbourfront": 14, "paya_lebar": 24, "expo": 42, "jurong_east": 18

    }

  },

  {

    district: "D04 (HarbourFront / Telok Blangah / Sentosa)",

    rentCondo: { "1": 3950, "2": 5300, "3": 8500, "4": 12500 },

    commuteMetrics: {

      "raffles_place": 14, "tanjong_pagar": 12, "downtown": 16, "shenton_way": 18,

      "one_north": 14, "buona_vista": 16, "bugis": 18, "city_hall": 16,

      "harbourfront": 4, "paya_lebar": 26, "expo": 44, "jurong_east": 26

    }

  },

  {

    district: "D05 (Buona Vista / West Coast / Clementi)",

    rentCondo: { "1": 3350, "2": 4150, "3": 5550, "4": 6800 },

    commuteMetrics: {

      "raffles_place": 22, "tanjong_pagar": 20, "downtown": 26, "shenton_way": 24,

      "one_north": 6, "buona_vista": 4, "bugis": 26, "city_hall": 24,

      "harbourfront": 18, "paya_lebar": 34, "expo": 52, "jurong_east": 10

    }

  },

  {

    district: "D06 (High Street / City Hall)",

    rentCondo: { "1": 4810 },

    commuteMetrics: {

      "raffles_place": 6, "tanjong_pagar": 8, "downtown": 8, "shenton_way": 10,

      "one_north": 22, "buona_vista": 20, "bugis": 4, "city_hall": 4,

      "harbourfront": 16, "paya_lebar": 12, "expo": 28, "jurong_east": 26

    }

  },

  {

    district: "D07 (Bugis / Beach Road / Golden Mile)",

    rentCondo: { "1": 4400, "2": 5100, "3": 6850 },

    commuteMetrics: {

      "raffles_place": 6, "tanjong_pagar": 8, "downtown": 8, "shenton_way": 10,

      "one_north": 22, "buona_vista": 20, "bugis": 4, "city_hall": 4,

      "harbourfront": 16, "paya_lebar": 12, "expo": 28, "jurong_east": 26

    }

  },

  {

    district: "D08 (Farrer Park / Serangoon Road)",

    rentCondo: { "1": 3250, "2": 4300, "3": 5542.5, "4": 7000 },

    commuteMetrics: {

      "raffles_place": 14, "tanjong_pagar": 16, "downtown": 12, "shenton_way": 14,

      "one_north": 26, "buona_vista": 24, "bugis": 8, "city_hall": 10,

      "harbourfront": 16, "paya_lebar": 18, "expo": 34, "jurong_east": 30

    }

  },

  {

    district: "D09 (Orchard / River Valley)",

    rentCondo: { "1": 4000, "2": 5500, "3": 8000, "4": 11400 },

    commuteMetrics: {

      "raffles_place": 10, "tanjong_pagar": 12, "downtown": 12, "shenton_way": 8,

      "one_north": 18, "buona_vista": 16, "bugis": 11, "city_hall": 8,

      "harbourfront": 15, "paya_lebar": 20, "expo": 36, "jurong_east": 24

    }

  },

  {

    district: "D10 (Tanglin / Holland / Bukit Timah)",

    rentCondo: { "1": 3700, "2": 5100, "3": 7300, "4": 13500, "5": 15950 },

    commuteMetrics: {

      "raffles_place": 18, "tanjong_pagar": 16, "downtown": 16, "shenton_way": 18,

      "one_north": 10, "buona_vista": 8, "bugis": 15, "city_hall": 14,

      "harbourfront": 18, "paya_lebar": 26, "expo": 40, "jurong_east": 16

    }

  },

  {

    district: "D11 (Novena / Newton / Thomson)",

    rentCondo: { "1": 3400, "2": 4500, "3": 6500, "4": 9500 },

    commuteMetrics: {

      "raffles_place": 12, "tanjong_pagar": 14, "downtown": 14, "shenton_way": 11,

      "one_north": 22, "buona_vista": 20, "bugis": 10, "city_hall": 10,

      "harbourfront": 22, "paya_lebar": 20, "expo": 36, "jurong_east": 26

    }

  },

  {

    district: "D12 (Toa Payoh / Balestier)",

    rentCondo: { "1": 3000, "2": 3800, "3": 4700, "4": 6300 },

    commuteMetrics: {

      "raffles_place": 16, "tanjong_pagar": 18, "downtown": 18, "shenton_way": 15,

      "one_north": 28, "buona_vista": 26, "bugis": 14, "city_hall": 14,

      "harbourfront": 26, "paya_lebar": 16, "expo": 32, "jurong_east": 30

    }

  },

  {

    district: "D13 (MacPherson / Braddell / Potong Pasir)",

    rentCondo: { "1": 3150, "2": 4100, "3": 5700, "4": 7350 },

    commuteMetrics: {

      "raffles_place": 18, "tanjong_pagar": 20, "downtown": 16, "shenton_way": 18,

      "one_north": 24, "buona_vista": 22, "bugis": 11, "city_hall": 13,

      "harbourfront": 24, "paya_lebar": 8, "expo": 24, "jurong_east": 34

    }

  },

  {

    district: "D14 (Paya Lebar / Geylang / Eunos)",

    rentCondo: { "1": 2900, "2": 3900, "3": 4800, "4": 6325 },

    commuteMetrics: {

      "raffles_place": 14, "tanjong_pagar": 16, "downtown": 22, "shenton_way": 24,

      "one_north": 29, "buona_vista": 27, "bugis": 10, "city_hall": 12,

      "harbourfront": 26, "paya_lebar": 4, "expo": 20, "jurong_east": 34

    }

  },

  {

    district: "D15 (East Coast / Marine Parade / Katong)",

    rentCondo: { "1": 3400, "2": 4500, "3": 6000, "4": 8400 },

    commuteMetrics: {

      "raffles_place": 22, "tanjong_pagar": 25, "downtown": 18, "shenton_way": 16,

      "one_north": 38, "buona_vista": 36, "bugis": 20, "city_hall": 22,

      "harbourfront": 26, "paya_lebar": 15, "expo": 25, "jurong_east": 46

    }

  },

  {

    district: "D16 (Bedok / Upper East Coast / Kew Drive)",

    rentCondo: { "1": 3100, "2": 3800, "3": 4800, "4": 6650 },

    commuteMetrics: {

      "raffles_place": 24, "tanjong_pagar": 26, "downtown": 28, "shenton_way": 26,

      "one_north": 40, "buona_vista": 38, "bugis": 20, "city_hall": 22,

      "harbourfront": 34, "paya_lebar": 12, "expo": 10, "jurong_east": 44

    }

  },

  {

    district: "D17 (Changi / Loyang / Changi Village)",

    rentCondo: { "1": 2650, "2": 3200, "3": 4000, "4": 4750 },

    commuteMetrics: {

      "raffles_place": 36, "tanjong_pagar": 38, "downtown": 42, "shenton_way": 44,

      "one_north": 52, "buona_vista": 50, "bugis": 32, "city_hall": 34,

      "harbourfront": 48, "paya_lebar": 24, "expo": 14, "jurong_east": 56

    }

  },

  {

    district: "D18 (Tampines / Pasir Ris)",

    rentCondo: { "1": 2788, "2": 3500, "3": 4350, "4": 5500 },

    commuteMetrics: {

      "raffles_place": 34, "tanjong_pagar": 36, "downtown": 42, "shenton_way": 45,

      "one_north": 54, "buona_vista": 52, "bugis": 30, "city_hall": 32,

      "harbourfront": 48, "paya_lebar": 20, "expo": 12, "jurong_east": 54

    }

  },

  {

    district: "D19 (Hougang / Sengkang / Punggol)",

    rentCondo: { "1": 2850, "2": 3500, "3": 4400, "4": 6000 },

    commuteMetrics: {

      "raffles_place": 28, "tanjong_pagar": 30, "downtown": 26, "shenton_way": 28,

      "one_north": 38, "buona_vista": 36, "bugis": 22, "city_hall": 24,

      "harbourfront": 32, "paya_lebar": 18, "expo": 38, "jurong_east": 44

    }

  },

  {

    district: "D20 (Bishan / Ang Mo Kio)",

    rentCondo: { "1": 3100, "2": 4000, "3": 5050, "4": 6600 },

    commuteMetrics: {

      "raffles_place": 20, "tanjong_pagar": 22, "downtown": 22, "shenton_way": 16,

      "one_north": 24, "buona_vista": 22, "bugis": 18, "city_hall": 16,

      "harbourfront": 28, "paya_lebar": 14, "expo": 36, "jurong_east": 32

    }

  },

  {

    district: "D21 (Upper Bukit Timah / Ulu Pandan)",

    rentCondo: { "1": 3000, "2": 3900, "3": 5000, "4": 6600 },

    commuteMetrics: {

      "raffles_place": 26, "tanjong_pagar": 24, "downtown": 22, "shenton_way": 24,

      "one_north": 14, "buona_vista": 12, "bugis": 20, "city_hall": 22,

      "harbourfront": 24, "paya_lebar": 32, "expo": 46, "jurong_east": 12

    }

  },

  {

    district: "D22 (Jurong / Boon Lay / Tuas)",

    rentCondo: { "1": 3500, "2": 4100, "3": 5200, "4": 6450 },

    commuteMetrics: {

      "raffles_place": 32, "tanjong_pagar": 30, "downtown": 36, "shenton_way": 38,

      "one_north": 18, "buona_vista": 16, "bugis": 34, "city_hall": 32,

      "harbourfront": 34, "paya_lebar": 42, "expo": 58, "jurong_east": 6

    }

  },

  {

    district: "D23 (Hillview / Choa Chu Kang / Bukit Panjang)",

    rentCondo: { "1": 3000, "2": 3600, "3": 4400, "4": 5800 },

    commuteMetrics: {

      "raffles_place": 34, "tanjong_pagar": 32, "downtown": 28, "shenton_way": 30,

      "one_north": 24, "buona_vista": 22, "bugis": 26, "city_hall": 28,

      "harbourfront": 36, "paya_lebar": 38, "expo": 52, "jurong_east": 14

    }

  }

];