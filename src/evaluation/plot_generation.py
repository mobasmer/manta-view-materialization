import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

plot_file_path = 'results/plots/'
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['xtick.labelsize'] = 14
plt.rcParams['ytick.labelsize'] = 14

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

    # create integer values for the x-axis
    xtick_values = [i for i in range(1, len(positions)+1)]
    # Plot the data
    plt.figure(figsize=(10, 6))
    sns.lineplot(x=positions, y=acc_sim, marker='o', linestyle='-', color='b', label='Accumulated similarity (avg)')
    sns.lineplot(x=positions, y=mmr_scores, marker='x', linestyle='--', color='r', label='MMR score')
    sns.lineplot(x=positions, y=max_scores, marker='s', linestyle='-.', color='g',
                 label='Max. sim. to prev. selected Views')
    plt.xlabel('k (Number of selected views)', fontsize=20)
    plt.ylabel('Score', fontsize=20)
    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)
    #plt.title(f'Development of Scores over k ({name})', fontsize=18)
    plt.legend(fontsize=16)
    plt.xticks(xtick_values)

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


def plot_runtime_breakdown(file_paths):
    labels = []
    index_time = []
    score_time = []
    view_time = []

    def extract_dataset_name(dataset_name):
        if dataset_name.startswith('data/'):
            return dataset_name.strip(".jsonocel").split('/')[-1]
        return dataset_name

    for file_path in file_paths:
        with open(file_path, 'r') as f:
            content = json.load(f)
            dataset = extract_dataset_name(content.get('filename', 'Unknown'))
            method = content.get('method', 'Unknown')
            runtimes = content.get('runtimes', {})

            labels.append(f"{dataset}\n{method}")
            index_time.append(runtimes.get('index_computation_time', 0))
            score_time.append(runtimes.get('score_computation_time', 0))
            view_time.append(runtimes.get('view_selection_time', 0))

    x = np.arange(len(labels))
    width = 0.6

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x, index_time, width, label='Index Computation Time')
    ax.bar(x, score_time, width, bottom=index_time, label='Score Computation Time')
    ax.bar(x, view_time, width, bottom=np.array(index_time) + np.array(score_time), label='View Selection Time')

    ax.set_xlabel('Dataset and Method')
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Stacked Bar Chart of Runtime Components')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45)
    ax.legend(title="Runtime Component")

    plt.tight_layout()
    plt.show()

def round_runtimes(file_names):
    for file in file_names:
        with open(file, 'r') as f:
            content = json.load(f)
            runtimes = content.get('runtimes', {})
            for key, value in runtimes.items():
                content['runtimes'][key] = round(value)

        with open(file.replace(".json", "-rounded.json"), 'w') as f:
            json.dump(content, f, indent=4)

if __name__ == '__main__':
    #plot_score_evolution('results/complete_results/20250317-205146_results_order_mmr-leading-type.json',"Order, Leading", "20250317-205146")
    #plot_score_evolution('results/complete_results/20250317-231201_results_BPI14_mmr-leading-type.json', "BPI14 (Sep), Leading", "20250317-231201")
    #plot_score_evolution('results/complete_results/20250318-132650_bpi14_mmr_interacting_entities_results.json',
    #                     "BPI14 (Sep), Interact", "20250318-132650_bpi14_mmr_interacting_entities")
    #plot_score_evolution("results/complete_results/20250319-080157_order_mmr_interacting_entities_results.json", "Order-P1-Interact", "20250319-080157_order_mmr_interacting_entities")
    #plot_score_evolution("results/complete_results/20250319-115511_bpi14_mmr_interacting_entities_results.json", "BPI14-P6-Interact", "20250319-115511_bpi14_mmr_interacting_entities")
    #plot_score_evolution("results/complete_results/20250319-120450_bpi14_mmr_interacting_entities_results.json", "BPI14-P10-Interact", "20250319-120450_bpi14_mmr_interacting_entities")
    plot_score_evolution('results/demonstration_results/20250317-231201_results_BPI14_mmr-leading-type.json',
                         "BPI14 (Sep), Leading", "20250317-231201")
    plot_score_evolution('results/demonstration_results/20250317-205146_results_order_mmr-leading-type.json',
                         "Order, Leading", "20250317-205146")
    plot_score_evolution("results/demonstration_results/20250613-151805_bpi14_mmr_interacting_entities_results.json",
                         "BPI14 (Sep), Interact", "20250613-151805_bpi14_mmr_interacting_entities")

    file_names = ['results/demonstration_results/20250317-205146_results_order_mmr-leading-type.json',
                    'results/demonstration_results/20250317-231201_results_BPI14_mmr-leading-type.json',
                    'results/demonstration_results/20250613-151805_bpi14_mmr_interacting_entities_results.json']
    round_runtimes(file_names)
    plot_runtime_breakdown(file_names)