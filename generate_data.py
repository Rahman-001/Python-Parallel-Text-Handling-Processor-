import pandas as pd
import numpy as np

# Settings
FILE_NAME = "large_sample_data.csv"
NUM_ROWS = 100000  # 100,000 rows to simulate work

print(f"Generating {NUM_ROWS} rows of dummy data...")

# Create a DataFrame with random text and ID
df = pd.DataFrame({
    'id': range(NUM_ROWS),
    'text_content': np.random.choice([
        "This is a product.", 
        "I am very happy with the service provided.", 
        "Terrible experience, I want a refund now!", 
        "The delivery was late but the item is okay.", 
        "Neutral sentiment here."
    ], NUM_ROWS)
})

# Save to CSV
df.to_csv(FILE_NAME, index=False)
print(f"Done! Created '{FILE_NAME}' with {NUM_ROWS} rows.")