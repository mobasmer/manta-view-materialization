import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from matplotlib.ticker import MaxNLocator

plot_file_path = 'results/plots/'
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

# Extract sim scores and positions
def plot_score_evolution(file_path, name, file_id):
    # Load the JSON data
    with open(file_path, 'r') as f:
        data = json.load(f)

    positions = [view['position']+1 for view in data['selected_views']]
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
    plt.xlabel('k (Number of Selected Views)', fontsize=16)
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

if __name__ == '__main__':
    plot_score_evolution('results/complete_results/20250317-205146_results_order_mmr-leading-type.json',"Order, Leading", "20250317-205146")
    plot_score_evolution('results/complete_results/20250317-231201_results_BPI14_mmr-leading-type.json', "BPI14 (Sep), Leading", "20250317-231201")
    plot_score_evolution('results/complete_results/20250318-132650_bpi14_mmr_interacting_entities_results.json',
                         "BPI14 (Sep), Interact", "20250318-132650_bpi14_mmr_interacting_entities")