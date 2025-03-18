import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


plot_file_path = 'results/plots/'
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

# Extract sim scores and positions
def plot_score_evolution(file_path, name, file_id):
    # Load the JSON data
    with open(file_path, 'r') as f:
        data = json.load(f)

    positions = [view['position'] for view in data['selected_views']]
    sim_scores = [view['score info']['sim_score'] for view in data['selected_views']]
    mmr_scores = [view['score info']['mmr_score'] for view in data['selected_views']]
    max_scores = [view['score info']['max_sim_to_prev'] for view in data['selected_views']]
    acc_sim = __get_accumulated_similarity(file_path, file_id)

    # Plot the data
    plt.figure(figsize=(10, 6))
    sns.lineplot(x=positions, y=acc_sim, marker='o', linestyle='-', color='b', label='Accumulated Similarity (Avg)')
    sns.lineplot(x=positions, y=mmr_scores, marker='x', linestyle='--', color='r', label='MMR Score')
    sns.lineplot(x=positions, y=max_scores, marker='s', linestyle='-.', color='g',
                 label='Max. Sim. to Prev. Selected Views')
    plt.xlabel('k (Iteration)', fontsize=16)
    plt.ylabel('Score', fontsize=16)
    plt.title(f'Development of Scores over k ({name})', fontsize=18)
    plt.legend(fontsize=14)

    # Customize the spines
    plt.grid(False)

    # Save the plot
    plt.savefig(f'{plot_file_path}{name}_score_evolution.pdf', dpi=300, format='pdf')

    plt.show()

def __get_accumulated_similarity(file_path, file_id):
    with open(file_path, 'r') as f:
        positions = json.load(f)

    with open(f'results/complete_results/{file_id}_pairwise_scores.json', 'r') as f:
        pairwise_scores = json.load(f)

    entity_types = [view['object_type'] for view in positions['selected_views']] # assuming we comp for k == len(object_types)
    acc_similarity_scores = []
    for i in range(len(positions['selected_views'])):
        sel_views_at_i = [view['object_type'] for view in positions['selected_views'] if view['position'] <= i]
        max_score_to_ent_for_sel = {ent: [] for ent in entity_types}
        for ot1, ot2, sim in pairwise_scores:
            if ot2 in sel_views_at_i:
                max_score_to_ent_for_sel[ot1].append(sim)
        acc_similarity = sum([max(max_score_to_ent_for_sel[ent]) for ent in entity_types])
        acc_similarity_scores.append(acc_similarity/len(entity_types))

    return acc_similarity_scores

def generate_plot_for_runtimes(datasets, approaches, runtimes):
    # Plot the data
    datasets = ["Order-Leading", "BPI14 (Sep) - Leading"]
    runtimes = [{
        "index_computation_time": 1090.7053277492523,
        "score_computation_time": 4125.248391151428,
        "view_selection_time": 9.918212890625e-05,
        "run_time": 5215.954256772995
    }, {
        "index_computation_time": 371.5523769855499,
        "score_computation_time": 325.90549182891846,
        "view_selection_time": 0.00011014938354492188,
        "run_time": 697.4579980373383
    }]
    generate_plot_for_runtimes_helper(datasets, runtimes)
    return

    # Extracting the values for each runtime component
    index_times = [runtime["index_computation_time"] for runtime in runtimes]
    score_times = [runtime["score_computation_time"] for runtime in runtimes]
    selection_times = [runtime["view_selection_time"] for runtime in runtimes]

    # Plotting the stacked bar plot
    fig, ax = plt.subplots(figsize=(10, 6))

    bar_width = 0.5
    bar1 = ax.bar(datasets, index_times, bar_width, label='Index Computation Time', color='skyblue')
    bar2 = ax.bar(datasets, score_times, bar_width, bottom=index_times, label='Score Computation Time',
                  color='lightgreen')
    bar3 = ax.bar(datasets, selection_times, bar_width, bottom=[i + j for i, j in zip(index_times, score_times)],
                  label='View Selection Time', color='lightcoral')

    ax.set_xlabel('Datasets', fontsize=14)
    ax.set_ylabel('Runtime (s)', fontsize=14)
    ax.set_title('Runtimes for Different Steps', fontsize=16)
    ax.legend()

    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot
    plt.savefig('results/plots/runtimes_stacked_barplot.pdf', dpi=300, format='pdf')

    plt.show()

def generate_plot_for_runtimes_helper(datasets, runtimes):
    # Extracting the values for each runtime component
    index_times = [runtime["index_computation_time"] for runtime in runtimes]
    score_times = [runtime["score_computation_time"] for runtime in runtimes]
    selection_times = [runtime["view_selection_time"] for runtime in runtimes]

    # Creating a DataFrame for easier plotting with Seaborn
    df = pd.DataFrame({
        'Dataset': datasets * 3,
        'Runtime': index_times + score_times + selection_times,
        'Component': ['Index Computation'] * len(datasets) +
                     ['Score Computation'] * len(datasets) +
                     ['View Selection'] * len(datasets)
    })

    # Plotting the stacked bar plot using Seaborn
    plt.figure(figsize=(10, 6))
    sns.set(style="whitegrid")
    sns.barplot(x='Dataset', y='Runtime', hue='Component', data=df, palette='viridis')

    plt.xlabel('Datasets', fontsize=16, fontweight='bold')
    plt.ylabel('Runtime (s)', fontsize=16, fontweight='bold')
    plt.title('Runtimes for Manta', fontsize=18, fontweight='bold')
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(title='Component', fontsize=12, title_fontsize='13')
    plt.tight_layout()

    # Save the plot
    plt.savefig('results/plots/runtimes_stacked_barplot.pdf', dpi=300, format='pdf')

    # Show the plot
    plt.show()

if __name__ == '__main__':
    #plot_score_evolution('results/complete_results/20250317-205146_results_order_mmr-leading-type.json',"Order, Leading", "20250317-205146")
    #plot_score_evolution('results/complete_results/20250317-231201_results_BPI14_mmr-leading-type.json', "BPI14 (Sep), Leading", "20250317-231201")
    generate_plot_for_runtimes(None, None, None)