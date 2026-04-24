import random
from datetime import datetime, timedelta

# Config
num_records = 1500
start_case = 900000

offenses = [
    "Trespassing", "Public Intoxication", "Identity Fraud",
    "Larceny", "Vandalism", "Underage Possession of Alcohol",
    "Assault", "Credit Card Fraud", "Disorderly Conduct",
    "Driving Under the Influence",

    "Petit Larceny", "Grand Larceny", "Fraud",
    "Motor Vehicle Theft", "Hit and Run", "Damage to Property",
    "Stalking", "Harassment", "Fake ID",
    "Reckless Driving",

    "Burglary", "Drug Possession", "Underage Possession of Marijuana",
    "Extortion", "Assault and Battery", "Violation of Protective Order",
    "Theft from Motor Vehicle", "Credit Card Theft", "Urinating in Public",
    "Unauthorized Use of Vehicle"
]

locations = [
    "Goodwin Hall", "Pritchard Hall", "Dietrick Hall",
    "Hoge Hall", "Slusher Hall", "McBryde Hall",
    "Newman Hall", "Owens Hall", "Johnson Student Center",
    "Cassell Coliseum",

    "Payne Hall", "Miles Hall", "Eggleston Hall",
    "Cochrane Hall", "Pearson Hall", "Hillcrest Hall",
    "Campbell Hall", "Vawter Hall", "West Ambler Johnston Hall",
    "New Residence Hall East",

    "War Memorial Hall", "Torgersen Hall", "Davidson Hall",
    "Burruss Hall", "Hahn Hall", "Lane Stadium",
    "Duck Pond Lot", "Perry Street Garage",
    "Architecture Annex", "Media Building Lot", 
    "Cookout", "BNB Theater", "VT Golf Course", "Oak Lane",
    
     
    "Main Street", "North Main Street", "South Main Street",
    "College Avenue", "Draper Road", "Roanoke Street",
    "Prices Fork Road", "Southgate Drive", "West Campus Drive",
    "Washington Street", "Duck Pond Drive", "Perry Street",
    "Kent Street", "Otey Street", "Progress Street",
    "Patrick Henry Drive", "Harding Avenue", "Broce Drive",
    "Turner Street", "Clay Street"
]

dispositions = [
    "Cleared by Arrest", "Active", "Inactive",
    "Inactive-Referred to Student Conduct"
]

def random_date(year=2025):
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def format_date(dt):
    return dt.strftime("%m/%d/%Y")

def random_time():
    # Weighted crime-hour distribution
    weighted_hours = (
        [0]*10 + [1]*10 + [2]*10 + [3]*8 +      # very late night
        [20]*7 + [21]*8 + [22]*10 + [23]*10 +   # late evening
        [18]*5 + [19]*5 +                       # evening
        [12]*3 + [13]*3 + [14]*3 +              # midday
        [8]*2 + [9]*2 + [10]*2 + [11]*2 +       # morning
        [4,5,6,7,15,16,17]                      # low-frequency filler
    )
    
    hour = random.choice(weighted_hours)
    minute = random.randint(0, 59)
    return f"{hour:02}:{minute:02}"

records = []

weighted_offenses = (
    ["Underage Possession of Alcohol"] * 18 +
    ["Public Intoxication"] * 10 +
    ["Petit Larceny"] * 7 +

    ["Larceny"] * 5 +
    ["Vandalism"] * 5 +
    ["Trespassing"] * 4 +
    ["Disorderly Conduct"] * 4 +
    ["Fake ID"] * 4 +
    ["Driving Under the Influence"] * 3 +
    ["Assault"] * 3 +

    ["Identity Fraud"] * 2 +
    ["Credit Card Fraud"] * 2 +
    ["Fraud"] * 2 +
    ["Hit and Run"] * 2 +
    ["Reckless Driving"] * 2 +
    ["Motor Vehicle Theft"] * 2 +

    ["Grand Larceny"] * 1 +
    ["Burglary"] * 1 +
    ["Drug Possession"] * 1 +
    ["Underage Possession of Marijuana"] * 1 +
    ["Extortion"] * 1 +
    ["Assault and Battery"] * 1 +
    ["Violation of Protective Order"] * 1 +
    ["Theft from Motor Vehicle"] * 1 +
    ["Credit Card Theft"] * 1 +
    ["Urinating in Public"] * 1 +
    ["Unauthorized Use of Vehicle"] * 1 +
    ["Harassment"] * 1 +
    ["Stalking"] * 1 +
    ["Damage to Property"] * 1
)

for i in range(num_records):
    case_number = f"2025-{start_case + i}"
    
    report_date = random_date(2025)

    # occurrence date happens on or before report date
    days_before = random.randint(0, 14)   # usually within 2 weeks
    occurrence_date = report_date - timedelta(days=days_before)
    
    offense = random.choice(weighted_offenses)    
    location = random.choice(locations)
    disposition = random.choice(dispositions)

    time = random_time()
    record = f"{case_number} {format_date(report_date)} {offense} {location} {format_date(occurrence_date)} {time} {disposition}"
    records.append(record)

with open("test_1500_incidents.txt", "w") as f:
    for r in records:
        f.write(r + "\n")

print("✅ Generated 1500 test incidents.")