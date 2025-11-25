# Handling of SM Characterization in 3Phi Platform

## Example dataset for SM 100690:
```
{
  "100690": {
    "Topology": {
      "Secondary Substation ID": "302894",
      "Transformer ID": "31510",
      "Feeder ID": "313533",
      "Cabinet ID": "296903"
    },
    "Dataset Availability": {
      "Available": true,
      "Contains Data": true,
      "Relative Length": 0.6648469487229479,
      "Absolute Length": 17050
    },
    "Data Quality": {
      "L1": {
        "V": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569,
            "Zero frac": 0.0,
            "Below Vlim frac": 0.00093841642228739,
            "Frozen frac": 0.004164222873900294,
            "Total corruption frac": 0.00563049853372434
          }
        },
        "P14": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569
          }
        },
        "P23": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569
          }
        },
        "Q12": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569
          }
        },
        "Q34": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569
          }
        }
      },
      "L2": {
        "V": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569,
            "Zero frac": 0.0,
            "Below Vlim frac": 0.0002932551319648094,
            "Frozen frac": 0.009853372434017595,
            "Total corruption frac": 0.010674486803519062
          }
        },
        "P14": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } },
        "P23": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } },
        "Q12": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } },
        "Q34": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } }
      },
      "L3": {
        "V": {
          "Summary": "Good",
          "Detailed": {
            "NaN frac": 0.0005278592375366569,
            "Zero frac": 0.0,
            "Below Vlim frac": 0.00035190615835777126,
            "Frozen frac": 0.007859237536656892,
            "Total corruption frac": 0.00873900293255132
          }
        },
        "P14": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } },
        "P23": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } },
        "Q12": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } },
        "Q34": { "Summary": "Good", "Detailed": { "NaN frac": 0.0005278592375366569 } }
      }
    },
    "Data Statistics": {
      "L1": {
        "V": { "Min": 203.0, "Max": 249.0, "Mean": 234.90933227539062, "Std": 10.440539360046387 },
        "P14": { "Min": 0.0, "Max": 726.0, "Mean": 107.0615005493164, "Std": 108.94902801513672 },
        "P23": { "Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0 },
        "Q12": { "Min": 0.0, "Max": 6.0, "Mean": 0.01185376476496458, "Std": 0.14323671162128448 },
        "Q34": { "Min": 21.0, "Max": 161.0, "Mean": 73.14940643310547, "Std": 17.01614761352539 }
      },
      "L2": {
        "V": { "Min": 206.0, "Max": 249.0, "Mean": 236.00721740722656, "Std": 10.518004417419434 },
        "P14": { "Min": 0.0, "Max": 1216.0, "Mean": 140.78135681152344, "Std": 131.04580688476562 },
        "P23": { "Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0 },
        "Q12": { "Min": 0.0, "Max": 210.0, "Mean": 0.08878587186336517, "Std": 3.6756131649017334 },
        "Q34": { "Min": 1.0, "Max": 9.0, "Mean": 8.218942642211914, "Std": 1.0214864015579224 }
      },
      "L3": {
        "V": { "Min": 205.0, "Max": 250.0, "Mean": 236.0614471435547, "Std": 10.227823257446289 },
        "P14": { "Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0 },
        "P23": { "Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0 },
        "Q12": { "Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0 },
        "Q34": { "Min": 3.0, "Max": 7.0, "Mean": 6.081039905548096, "Std": 0.688140869140625 }
      }
    },
    "Connectivity": {
      "Connected Phases": ["L1", "L2", "L3"],
      "Connection Error": [],
      "Switching Phases": []
    }
  }
}

```

## Topology

```
"Topology": {
  "Secondary Substation ID": "302894",
  "Transformer ID": "31510",
  "Feeder ID": "313533",
  "Cabinet ID": "296903"
}
```

This Information can be queried using the 3phi-frameworks [TopologyController](https://gitlab.3pi-dev.io/3phaseinsight/3phi-framework/-/blob/main/src/threephi_framework/controllers/topology.py?ref_type=heads).

## Dataset Availability

```
"Dataset Availability": {
  "Available": true,
  "Contains Data": true,
  "Relative Length": 0.6648469487229479,
  "Absolute Length": 17050
}
```

This can be deduced from the "meter" table. A "MeterController" should be available for this. The boolean "Available"
is deduced from whether the meter_id is found in the table, "Contains Data" will be true if `total_rows` > 0, 
"Absolute Length" is the number of `total_rows` and the "Relative Length" is the quotient of `total_rows` divided by the 
maximum number of rows available for any meter.

## Data Quality

```
"Data Quality": {
  "L1": {
    "V": {
      "Summary": "Good",
      "Detailed": {
        "NaN frac": 0.0005278592375366569,
        "Zero frac": 0.0,
        "Below Vlim frac": 0.00093841642228739,
        "Frozen frac": 0.004164222873900294,
        "Total corruption frac": 0.00563049853372434
      }
    },
    "P14": {
      ...
    },
    "P23": {
      ...
    },
    "Q12": {
      ...
    },
    "Q34": {
      ...
    }
  },
  "L2": {
    ...
  },
  "L3": {
    ...
  }
},
```

A dedicated "data_quality" column will be added to the `meter` table in order to be able to query this information quickly.

## Data Statistics

```
"Data Statistics": {
  "L1": {
    "V": { "Min": 203.0, "Max": 249.0, "Mean": 234.90933227539062, "Std": 10.440539360046387 },
    "P14": { "Min": 0.0, "Max": 726.0, "Mean": 107.0615005493164, "Std": 108.94902801513672 },
    "P23": { "Min": 0.0, "Max": 0.0, "Mean": 0.0, "Std": 0.0 },
    "Q12": { "Min": 0.0, "Max": 6.0, "Mean": 0.01185376476496458, "Std": 0.14323671162128448 },
    "Q34": { "Min": 21.0, "Max": 161.0, "Mean": 73.14940643310547, "Std": 17.01614761352539 }
  },
  "L2": {
    ...
  },
  "L3": {
    ...
  }
},
```

A dedicated "data_statistics" column will be added to the `meter` table in order to enable quick querying.

## Connectivity

```
"Connectivity": {
  "Connected Phases": ["L1", "L2", "L3"],
  "Connection Error": [],
  "Switching Phases": []
}
```

A dedicated "connectivity" column will be added to the `meter` table in order to enable quick querying.
