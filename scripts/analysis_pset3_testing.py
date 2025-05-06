import pandas as pd
import matplotlib.pyplot as plt
import os

def result_analysis(input_dir="results/results_bm_03/16_decent_results", output_dir="analysis_results"):
    tasks = ['fb', 'osmc', 'books']
    indexs = ['HybridPGMLIPP', 'DynamicPGM', 'LIPP']
    colors = {'HybridPGMLIPP': 'orange', 'DynamicPGM': 'blue', 'LIPP': 'red'}
    
    # Create dictionaries to store data
    throughput_10p = {idx: {} for idx in indexs}
    throughput_90p = {idx: {} for idx in indexs}
    size_10p = {idx: {} for idx in indexs}
    size_90p = {idx: {} for idx in indexs}
    
    # Process each dataset
    for task in tasks:
        full_task_name = f"{task}_100M_public_uint64"
        
        # Read 10% insert ratio results
        mix_10p_results = pd.read_csv(f"{input_dir}/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix_results_table.csv")
        # Read 90% insert ratio results
        mix_90p_results = pd.read_csv(f"{input_dir}/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix_results_table.csv")
        
        # Process each index
        for idx in indexs:
            # Process 10% insert ratio
            try:
                idx_results = mix_10p_results[mix_10p_results['index_name'] == idx]
                throughput_10p[idx][task] = idx_results[['mixed_throughput_mops1', 'mixed_throughput_mops2', 'mixed_throughput_mops3']].mean(axis=1).max()
                size_10p[idx][task] = idx_results['index_size_bytes'].iloc[0]
            except:
                pass
            
            # Process 90% insert ratio
            try:
                idx_results = mix_90p_results[mix_90p_results['index_name'] == idx]
                throughput_90p[idx][task] = idx_results[['mixed_throughput_mops1', 'mixed_throughput_mops2', 'mixed_throughput_mops3']].mean(axis=1).max()
                size_90p[idx][task] = idx_results['index_size_bytes'].iloc[0]
            except:
                pass
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # --- Combined subplot for index size (10% insert ratio) ---
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    positions = range(len(indexs))
    for i, task in enumerate(['books', 'fb', 'osmc']):
        task_data = [size_10p[idx].get(task, 0) / (1024*1024) for idx in indexs]
        axs[i].bar(positions, task_data, color=[colors[idx] for idx in indexs])
        axs[i].set_title(f"{task.upper()} Dataset")
        axs[i].set_ylabel('Size (MB)')
        axs[i].set_xticks(positions)
        axs[i].set_xticklabels(indexs, rotation=45)
        axs[i].set_ylim(bottom=0)
    fig.suptitle('Index Size Comparison (10% Insert Ratio)')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f'{output_dir}/size_comparison_10p.png', dpi=300)
    plt.close()

    # --- Combined subplot for index size (90% insert ratio) ---
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    for i, task in enumerate(['books', 'fb', 'osmc']):
        task_data = [size_90p[idx].get(task, 0) / (1024*1024) for idx in indexs]
        axs[i].bar(positions, task_data, color=[colors[idx] for idx in indexs])
        axs[i].set_title(f"{task.upper()} Dataset")
        axs[i].set_ylabel('Size (MB)')
        axs[i].set_xticks(positions)
        axs[i].set_xticklabels(indexs, rotation=45)
        axs[i].set_ylim(bottom=0)
    fig.suptitle('Index Size Comparison (90% Insert Ratio)')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f'{output_dir}/size_comparison_90p.png', dpi=300)
    plt.close()

    # --- Combined subplot for throughput (10% insert ratio) ---
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    for i, task in enumerate(['books', 'fb', 'osmc']):
        task_data = [throughput_10p[idx].get(task, 0) for idx in indexs]
        axs[i].bar(positions, task_data, color=[colors[idx] for idx in indexs])
        axs[i].set_title(f"{task.upper()} Dataset")
        axs[i].set_ylabel('Throughput (Mops/s)')
        axs[i].set_xticks(positions)
        axs[i].set_xticklabels(indexs, rotation=45)
        axs[i].set_ylim(bottom=0)
    fig.suptitle('Throughput Comparison (10% Insert Ratio)')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f'{output_dir}/throughput_comparison_10p.png', dpi=300)
    plt.close()

    # --- Combined subplot for throughput (90% insert ratio) ---
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    for i, task in enumerate(['books', 'fb', 'osmc']):
        task_data = [throughput_90p[idx].get(task, 0) for idx in indexs]
        axs[i].bar(positions, task_data, color=[colors[idx] for idx in indexs])
        axs[i].set_title(f"{task.upper()} Dataset")
        axs[i].set_ylabel('Throughput (Mops/s)')
        axs[i].set_xticks(positions)
        axs[i].set_xticklabels(indexs, rotation=45)
        axs[i].set_ylim(bottom=0)
    fig.suptitle('Throughput Comparison (90% Insert Ratio)')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f'{output_dir}/throughput_comparison_90p.png', dpi=300)
    plt.close()

    # (Optional) Continue to save individual plots as before, but now with new colors
    for task in tasks:
        # Size plots for 10% insert ratio
        plt.figure(figsize=(10, 6))
        task_data = [size_10p[idx].get(task, 0) / (1024*1024) for idx in indexs]
        plt.bar(indexs, task_data, color=[colors[idx] for idx in indexs])
        plt.title(f'Index Size - {task.upper()} Dataset (10% Insert Ratio)')
        plt.ylabel('Size (MB)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/size_{task}_10p.png', dpi=300)
        plt.close()
    
    # Save data to CSV files
    pd.DataFrame(throughput_10p).to_csv(f'{output_dir}/throughput_10p.csv')
    pd.DataFrame(throughput_90p).to_csv(f'{output_dir}/throughput_90p.csv')
    pd.DataFrame(size_10p).to_csv(f'{output_dir}/size_10p.csv')
    pd.DataFrame(size_90p).to_csv(f'{output_dir}/size_90p.csv')

if __name__ == "__main__":
    result_analysis()
        
    