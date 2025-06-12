from src.util.similarity_measures import matching_similarities
from src.view_generation.ekg_leading_type import batches

views = [
    ({(1,2): [0,1,2], (2,3): [0,2], (3,4): [1,2]}, 3),
    ({(1,2): [0,1,2], (5,3): [0,2], (3,4): [0]}, 3)
]

print(matching_similarities(views[0], views[1]))

views_same = [
    ({(1,2): [0,1,2], (2,3): [0,2], (3,4): [1,2]}, 3),
    ({(1,2): [0,1,2], (2,3): [0,2], (3,4): [1,2]}, 3),
]

print(matching_similarities(views_same[0], views_same[1]))

views_same_same_but_different = [
    ({(1,2): [0], (2,3): [0], (3,4): [0]}, 3),
    ({(1,2): [0,1,2], (2,3): [0,1,2], (3,4): [0,1,2]}, 3),
]

print(matching_similarities(views_same_same_but_different[0], views_same_same_but_different[1]))
