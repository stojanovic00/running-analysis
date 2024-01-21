# Sample data
data = [
    {"Runner ID": 1, "Average Speed": 8.346, "Year of Run": 2018},
    {"Runner ID": 1, "Average Speed": 9.469, "Year of Run": 2019},
    {"Runner ID": 1, "Average Speed": 9.599, "Year of Run": 2020},
    {"Runner ID": 1, "Average Speed": 10.433, "Year of Run": 2021},
    {"Runner ID": 1, "Average Speed": 12.014, "Year of Run": 2022},
]

# Calculate weighted average
current_year = 2022  # Replace this with the actual current year
max_years = current_year - min(entry["Year of Run"] for entry in data) + 1

weighted_sum = 0
total_weight = 0

for entry in data:
    weight = 1 - (current_year - entry["Year of Run"]) / max_years
    weighted_sum += entry["Average Speed"] * weight
    total_weight += weight

weighted_average = weighted_sum / total_weight

print("Weighted Average Speed:", weighted_average)
