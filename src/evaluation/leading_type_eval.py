from src.strategies.subset_selection import SubsetSelector
from src.view_generation.leading_type import compute_edges_by_leading_type


def main():
    compute_views_for_bpi17()

def compute_views_for_bpi17():
    filename = '../../data/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource"]
    act_name = "event_activity"
    time_name = "event_timestamp"
    sep = ","

    edges_leading_types = compute_edges_by_leading_type(filename, object_types, act_name, time_name, sep)

    subset_selection = SubsetSelector(views=edges_leading_types, weight=0.5)
    selected_views = subset_selection.select_view_indices(2)

    print(selected_views)
    return selected_views


def compute_views_for_bpi14():
    # filename = '../../data/Detail Change - wrapped.csv'
    # object_types = ["CI Name (aff)", "CI Type (aff)", "CI Subtype (aff)", "Service Component WBS (aff)", "Change ID", "Risk Assessment", "Emergency Change", "CAB-approval needed"]
    pass


if __name__ == "__main__":
    main()
