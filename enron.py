import os
import json
import csv
from pprint import pprint


sample_data_path = "maildir_sample"
full_data_path = "maildir_full"

# https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory


def generate_results(base_dir):
    email_results = []
    for (dirpath, dirnames, filenames) in os.walk(base_dir):
        for name in filenames:
            if name[len(name)-3:] != "swp":
                email_data = parse_email_file(filepath=os.path.join(dirpath, name))
                email_results.append(email_data)
    return email_results


def parse_email_file(filepath):

    with open(filepath) as file:
        email_data = file.read()

        first_index = 0
        # find the end of the metadata block first to avoid edge case bugs in email text
        x_filename = email_data.index("X-FileName: ")
        metadata_end = email_data.index("\n", x_filename)

        message_start_raw = email_data.index("Message-ID: ", first_index, metadata_end)
        message_start = message_start_raw + len("Message-ID: ")

        date_start_raw = email_data.index("Date: ", first_index, metadata_end)
        date_start = date_start_raw + len("Date: ")

        from_start_raw = email_data.index("From: ", first_index, metadata_end)
        from_start = from_start_raw + len("From: ")

        to_start_raw = email_data.index("To: ", first_index, metadata_end)
        to_start = to_start_raw + len("To: ")
        subject_start_raw = email_data.index("Subject: ", first_index, metadata_end)

        message_id = email_data[message_start:date_start_raw]
        date = email_data[date_start:from_start_raw]

        to_line = email_data[to_start: subject_start_raw]
        text_body = email_data[metadata_end+1: len(email_data)-1]

        # this block fixes a bug where some emails don't have a To line
        if "\nTo: " in email_data:
            from_line = email_data[from_start: to_start_raw]
        else:
            from_line = email_data[from_start: subject_start_raw]

        final_data = {
            "Message-ID": message_id.strip(),
            "Date": date.strip(),
            "From": from_line.strip(),
            "To": to_line.strip(),
            "Message": text_body
        }

        return final_data


def export_results(export_filename, results_data, to_csv=True):

    if to_csv:
        with open(export_filename, 'w') as csv_file_export:
            fieldnames = ["Message-ID", "Date", "From", "To", "Message"]
            writer = csv.DictWriter(csv_file_export, fieldnames=fieldnames)

            writer.writeheader()
            for row in results_data:
                writer.writerow(row)
    else:
        with open(export_filename, 'w') as fp:
            json.dump(results_data, fp)


def run_pipeline(enron_data_dir, export_csv_file):

    results = generate_results(base_dir=enron_data_dir)
    export_results(export_filename=export_csv_file, results_data=results)

run_pipeline(enron_data_dir=sample_data_path, export_csv_file="sample.csv")
