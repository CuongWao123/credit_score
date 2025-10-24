import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os
from pathlib import Path

def split_training_data(
    input_file: str,
    output_dir: str = "./data/split",
    train_ratio: float = 0.9,
    target_column: str = "TARGET",
    random_state: int = 42
):
    """
    Split training data into 90% train and 10% simulation data
    
    Args:
        input_file: Path to input training CSV file
        output_dir: Directory to save output files
        train_ratio: Ratio for training data (default 0.9 = 90%)
        target_column: Name of target column to remove from simulation data
        random_state: Random seed for reproducibility
    """
    
    print("=" * 80)
    print("📊 SPLITTING TRAINING DATASET")
    print("=" * 80)
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Read training data
    print(f"\n📁 Reading data from: {input_file}")
    df = pd.read_csv(input_file)
    print(f"✅ Loaded {len(df):,} rows and {len(df.columns)} columns")
    
    # Check if target column exists
    if target_column not in df.columns:
        raise ValueError(f"Target column '{target_column}' not found in dataset!")
    
    # Display target distribution
    print(f"\n📈 Target distribution:")
    target_dist = df[target_column].value_counts()
    for label, count in target_dist.items():
        percentage = (count / len(df)) * 100
        print(f"   Class {label}: {count:,} ({percentage:.2f}%)")
    
    # Split data with stratification to maintain target distribution
    print(f"\n✂️ Splitting data: {train_ratio*100:.0f}% train, {(1-train_ratio)*100:.0f}% simulation")
    
    train_data, sim_data = train_test_split(
        df,
        train_size=train_ratio,
        random_state=random_state,
        stratify=df[target_column]  # Maintain target distribution
    )
    
    print(f"   Train data: {len(train_data):,} rows")
    print(f"   Simulation data: {len(sim_data):,} rows")
    
    # Verify stratification
    print(f"\n✓ Train data target distribution:")
    train_dist = train_data[target_column].value_counts()
    for label, count in train_dist.items():
        percentage = (count / len(train_data)) * 100
        print(f"   Class {label}: {count:,} ({percentage:.2f}%)")
    
    print(f"\n✓ Simulation data target distribution (before removing target):")
    sim_dist = sim_data[target_column].value_counts()
    for label, count in sim_dist.items():
        percentage = (count / len(sim_data)) * 100
        print(f"   Class {label}: {count:,} ({percentage:.2f}%)")
    
    # Save training data with target
    train_output = os.path.join(output_dir, "train_data.csv")
    print(f"\n💾 Saving training data to: {train_output}")
    train_data.to_csv(train_output, index=False)
    print(f"✅ Saved {len(train_data):,} rows with {len(train_data.columns)} columns")
    
    # Save simulation data WITHOUT target (for real-time prediction simulation)
    sim_features = sim_data.drop(columns=[target_column])
    sim_output = os.path.join(output_dir, "production.csv")
    print(f"\n💾 Saving simulation data (without target) to: {sim_output}")
    sim_features.to_csv(sim_output, index=False)
    print(f"✅ Saved {len(sim_features):,} rows with {len(sim_features.columns)} columns")
    
    # Save simulation data WITH target (for validation/evaluation)
    sim_with_target_output = os.path.join(output_dir, "production_with_target.csv")
    print(f"\n💾 Saving simulation data (with target) to: {sim_with_target_output}")
    sim_data.to_csv(sim_with_target_output, index=False)
    print(f"✅ Saved {len(sim_data):,} rows with {len(sim_data.columns)} columns")
    
    # Create metadata file
    metadata = {
        "input_file": input_file,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "train_ratio": train_ratio,
        "train_rows": len(train_data),
        "simulation_rows": len(sim_data),
        "target_column": target_column,
        "random_state": random_state,
        "columns": list(df.columns),
        "target_distribution": df[target_column].value_counts().to_dict(),
        "train_target_distribution": train_data[target_column].value_counts().to_dict(),
        "simulation_target_distribution": sim_data[target_column].value_counts().to_dict()
    }
    
    metadata_output = os.path.join(output_dir, "split_metadata.json")
    print(f"\n📋 Saving metadata to: {metadata_output}")
    import json
    with open(metadata_output, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print("\n" + "=" * 80)
    print("✅ SPLIT COMPLETE!")
    print("=" * 80)
    print(f"\n📂 Output files:")
    print(f"   1. {train_output}")
    print(f"      → Use for model training (90% of data)")
    print(f"   2. {sim_output}")
    print(f"      → Use for real-time prediction simulation (10% of data, NO target)")
    print(f"   3. {sim_with_target_output}")
    print(f"      → Use for validation/evaluation (10% of data, WITH target)")
    print(f"   4. {metadata_output}")
    print(f"      → Metadata about the split")
    print("=" * 80 + "\n")
    
    return {
        "train_data": train_data,
        "simulation_data_no_target": sim_features,
        "simulation_data_with_target": sim_data,
        "metadata": metadata
    }


def analyze_split_data(split_dir: str = "./data/split"):
    """
    Analyze the split data files
    """
    import json
    
    print("=" * 80)
    print("📊 ANALYZING SPLIT DATA")
    print("=" * 80)
    
    # Read metadata
    metadata_file = os.path.join(split_dir, "split_metadata.json")
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        print(f"\n📋 Metadata:")
        print(f"   Original file: {metadata['input_file']}")
        print(f"   Total rows: {metadata['total_rows']:,}")
        print(f"   Total columns: {metadata['total_columns']}")
        print(f"   Train ratio: {metadata['train_ratio']*100:.0f}%")
        print(f"   Train rows: {metadata['train_rows']:,}")
        print(f"   Simulation rows: {metadata['simulation_rows']:,}")
        print(f"   Target column: {metadata['target_column']}")
        print(f"   Random state: {metadata['random_state']}")
    
    # Check files
    files = {
        "Training data (90%)": "train_data_90pct.csv",
        "Simulation data (no target)": "simulation_data_10pct_no_target.csv",
        "Simulation data (with target)": "simulation_data_10pct_with_target.csv"
    }
    
    print(f"\n📂 Files:")
    for name, filename in files.items():
        filepath = os.path.join(split_dir, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"   ✅ {name}")
            print(f"      - Path: {filepath}")
            print(f"      - Rows: {len(df):,}")
            print(f"      - Columns: {len(df.columns)}")
            print(f"      - Size: {size_mb:.2f} MB")
        else:
            print(f"   ❌ {name} - File not found")
    
    print("=" * 80 + "\n")


if __name__ == "__main__":
    # Configuration
    INPUT_FILE = "../raw_data/train.csv"  
    OUTPUT_DIR = "../raw_data/split"
    TRAIN_RATIO = 0.999
    TARGET_COLUMN = "target"
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: Input file not found: {INPUT_FILE}")
        print(f"\n💡 Please update INPUT_FILE in the script to point to your training data")
        exit(1)
    
    # Split the data
    result = split_training_data(
        input_file=INPUT_FILE,
        output_dir=OUTPUT_DIR,
        train_ratio=TRAIN_RATIO,
        target_column=TARGET_COLUMN,
        random_state=42
    )
    

    print("\n✅ Done! You can now use:")
    print("   - train_data.csv for model training")
    print("   - production.csv for real-time prediction simulation")
    print("   - production_with_target.csv for validation/evaluation")