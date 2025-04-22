import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import glob

def load_and_process_data():
    print("\n=== Starting Data Loading ===")
    # Load all CSV files
    files = glob.glob('results/*.csv')
    print(f"Found {len(files)} CSV files")
    print("Files found:", files)
    
    all_data = []
    
    # Define the indexes we want to analyze
    target_indexes = ['HybridPGMLIPP', 'DynamicPGM', 'LIPP']
    print("Target indexes:", target_indexes)
    
    for file in files:
        if 'fb_100M_public_uint64' in file:  # Only process Facebook dataset
            try:
                print(f"\nProcessing file: {file}")
                # Read CSV with flexible column handling
                df = pd.read_csv(file, on_bad_lines='skip')
                print("Raw data shape:", df.shape)
                print("Columns:", df.columns.tolist())
                print("Unique indexes in file:", df['index_name'].unique())
                
                # Skip empty dataframes
                if df.empty:
                    print("Warning: Empty dataframe")
                    continue
                
                # Filter for only the indexes we want
                df = df[df['index_name'].isin(target_indexes)]
                print("After filtering indexes:")
                print("Data shape:", df.shape)
                print("Remaining indexes:", df['index_name'].unique())
                
                if df.empty:
                    print("No matching indexes found in this file")
                    continue
                
                # Extract workload type from filename
                if '0.000000i' in file and not 'mix' in file:
                    workload = 'lookup_only'
                elif '0.500000i_0m' in file and not 'mix' in file:
                    workload = 'insert_lookup'
                elif '0.900000i_0m_mix' in file:
                    workload = 'mixed_90_insert'
                elif '0.100000i_0m_mix' in file:
                    workload = 'mixed_10_insert'
                else:
                    print("Skipping file - no matching workload pattern")
                    continue
                
                print(f"Workload type: {workload}")
                df['workload'] = workload
                all_data.append(df)
                print("Added to all_data")
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
                continue
    
    if not all_data:
        raise ValueError("No valid data found in CSV files")
    
    combined_data = pd.concat(all_data, ignore_index=True)
    print("\n=== Final Combined Data ===")
    print("Total rows:", len(combined_data))
    print("Available indexes:", combined_data['index_name'].unique())
    print("Available workloads:", combined_data['workload'].unique())
    print("\nData sample:")
    print(combined_data[['index_name', 'workload']].head())
    
    return combined_data

def plot_throughput(data, workload, ax):
    print(f"\n=== Plotting Throughput for {workload} ===")
    # Filter data for specific workload
    workload_data = data[data['workload'] == workload]
    print(f"Rows in workload data: {len(workload_data)}")
    
    if workload_data.empty:
        print(f"No data found for workload: {workload}")
        print("Available workloads:", data['workload'].unique())
        return
    
    # Get unique indexes
    indexes = workload_data['index_name'].unique()
    print("Indexes in workload:", indexes)
    
    # Calculate average throughput
    throughputs = []
    valid_indexes = []
    
    for idx in indexes:
        try:
            idx_data = workload_data[workload_data['index_name'] == idx]
            print(f"\nProcessing index: {idx}")
            print("Rows for this index:", len(idx_data))
            print("Available columns:", idx_data.columns.tolist())
            
            # Determine which columns to use based on workload type
            if 'mixed' in workload:
                # For mixed workloads, use mixed_throughput columns
                if 'mixed_throughput_mops1' in idx_data.columns:
                    throughput_cols = ['mixed_throughput_mops1', 'mixed_throughput_mops2', 'mixed_throughput_mops3']
                else:
                    print(f"Warning: No mixed throughput columns found for {idx} in {workload}")
                    continue
            else:
                # For non-mixed workloads, use lookup_throughput columns
                if 'lookup_throughput_mops1' in idx_data.columns:
                    throughput_cols = ['lookup_throughput_mops1', 'lookup_throughput_mops2', 'lookup_throughput_mops3']
                else:
                    print(f"Warning: No lookup throughput columns found for {idx} in {workload}")
                    continue
            
            print("Using columns:", throughput_cols)
            # Calculate average throughput
            avg_throughput = idx_data[throughput_cols].mean().mean()
            print(f"Calculated throughput: {avg_throughput}")
            
            # Check if the value is finite
            if np.isfinite(avg_throughput):
                throughputs.append(avg_throughput)
                valid_indexes.append(idx)
                print(f"Added {idx} with throughput {avg_throughput}")
            else:
                print(f"Warning: Non-finite throughput value for {idx}")
        except Exception as e:
            print(f"Error processing index {idx} for workload {workload}: {str(e)}")
            continue
    
    if not valid_indexes:
        print(f"No valid throughput data for workload: {workload}")
        return
    
    print(f"Final valid indexes for plotting: {valid_indexes}")
    print(f"Final throughput values: {throughputs}")
    
    # Create bar plot
    colors = {
        'HybridPGMLIPP': '#1f77b4',
        'DynamicPGM': '#ff7f0e',
        'LIPP': '#2ca02c'
    }
    bar_colors = [colors[idx] for idx in valid_indexes]
    bars = ax.bar(valid_indexes, throughputs, color=bar_colors)
    ax.set_title(f'Throughput - {workload.replace("_", " ").title()}', fontsize=12)
    ax.set_ylabel('Throughput (M ops/sec)', fontsize=10)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, linestyle='--', alpha=0.7)
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom', fontsize=9)

def plot_index_size(data, workload, ax):
    print(f"\n=== Plotting Index Size for {workload} ===")
    # Filter data for specific workload
    workload_data = data[data['workload'] == workload]
    print(f"Rows in workload data: {len(workload_data)}")
    
    if workload_data.empty:
        print(f"No data found for workload: {workload}")
        return
    
    # Get unique indexes
    indexes = workload_data['index_name'].unique()
    print("Indexes in workload:", indexes)
    
    # Get index sizes
    sizes = []
    valid_indexes = []
    
    for idx in indexes:
        try:
            if idx == 'DynamicPGM':
                # For DPGM, get the best performing variant
                dpgm_data = workload_data[workload_data['index_name'] == 'DynamicPGM']
                size = dpgm_data['index_size_bytes'].min()  # Use min size for DPGM
            else:
                size = workload_data[workload_data['index_name'] == idx]['index_size_bytes'].iloc[0]
            
            # Convert to MB and check if finite
            size_mb = size / (1024 * 1024)
            if np.isfinite(size_mb):
                sizes.append(size_mb)
                valid_indexes.append(idx)
                print(f"Added {idx} with size {size_mb:.2f} MB")
        except Exception as e:
            print(f"Error processing index {idx} for workload {workload}: {str(e)}")
            continue
    
    if not valid_indexes:
        print(f"No valid size data for workload: {workload}")
        return
    
    print(f"Final valid indexes for plotting: {valid_indexes}")
    print(f"Final size values: {sizes}")
    
    # Create bar plot
    colors = {
        'HybridPGMLIPP': '#1f77b4',
        'DynamicPGM': '#ff7f0e',
        'LIPP': '#2ca02c'
    }
    bar_colors = [colors[idx] for idx in valid_indexes]
    bars = ax.bar(valid_indexes, sizes, color=bar_colors)
    ax.set_title(f'Index Size - {workload.replace("_", " ").title()}', fontsize=12)
    ax.set_ylabel('Size (MB)', fontsize=10)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, linestyle='--', alpha=0.7)
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=9)

def main():
    try:
        # Load and process data
        data = load_and_process_data()
        
        # Create figure with subplots
        fig, axes = plt.subplots(4, 2, figsize=(15, 20))
        fig.suptitle('Facebook Dataset Performance Comparison\nHybridPGMLIPP vs DynamicPGM vs LIPP', fontsize=16, y=0.99)
        
        # Define workloads
        workloads = ['lookup_only', 'insert_lookup', 'mixed_90_insert', 'mixed_10_insert']
        
        # Plot throughput and size for each workload
        for i, workload in enumerate(workloads):
            plot_throughput(data, workload, axes[i, 0])
            plot_index_size(data, workload, axes[i, 1])
        
        # Adjust layout and save
        plt.tight_layout()
        # Save in the same directory as the script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, 'milestone2_results.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_path}")
        plt.close()
        
        # Save the processed data to CSV
        output_csv_path = os.path.join(script_dir, 'results', 'result_analysis.csv')
        data.to_csv(output_csv_path, index=False)
        print(f"Data saved to: {output_csv_path}")
        
    except Exception as e:
        print(f"Error in main: {str(e)}")

if __name__ == '__main__':
    main()
        


        
        
        
        
    