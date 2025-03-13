import csv


def wrap_columns_in_list(input_file, output_file, column_names):
    """
    Reads a CSV file, wraps each value in the specified column in a list, and saves it back as a new CSV file.
    """
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter=',')
        fieldnames = reader.fieldnames

        for column_name in column_names:
            if column_name not in fieldnames:
                raise ValueError(f"Column '{column_name}' not found in CSV file.")

        rows = []
        for row in reader:
            for column_name in column_names:
                row[column_name] = [row[column_name]]  # Wrap value in list
                rows.append(row)

    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

if __name__ == '__main__':
    wrap_columns_in_list('../../data/BPI2017-Final-adapt.csv', '../../data/BPI2017-Final-adapt.csv', ['event_EventID'])