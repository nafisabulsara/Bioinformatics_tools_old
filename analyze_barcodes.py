import os
import sys
import time
import argparse
import numpy as np
import pandas as pd
import pyfastx
import operator
from collections import defaultdict
from matplotlib import pyplot as plt


def reverse_complement(s):
    """
    Find reverse complements of the barcode
    :param s: string of barcode
    :return: reverse complement of barcode in
    """
    # Create a mapping dictionary
    # Time complexity: O(n)
    base_complement = dict(A='T', T='A', G='C', C='G', N='N')
    _letters = [base_complement[base] for base in list(s)]
    s = ''.join(_letters)
    return s[::-1]


def hamming_distance(str1, str2):
    """
    Function to calculate hamming distance between two strings (barcodes)
    :param str1: string 1 (barcode 1)
    :param str2: string 2 (barcode 2)
    :return: numeric value of hamming distance between the two strings
    """
    # Time complexity O(n)
    if len(str1) != len(str2):
        raise ValueError("Undefined for sequences of unequal length.")
    # Get the difference between each element in the two strings
    return sum(elem1 != elem2 for elem1, elem2 in zip(str1, str2))


def iterate_over_dict(barcode_dict):
    """
    Iterate over dictionary containing counts of unique
    barcodes to check for barcodes with one mismatch,
    and add their count to the 'true' barcode.
    :param barcode_dict: original barcodes dictionary
    containing counts and X/Y positions
    :return: final reduced barcodes dictionary
    """
    # sort in descending order of value to start with most frequent barcode
    sorted_barcode_dict = dict(sorted(barcode_dict.items(), key=operator.itemgetter(1), reverse=True))
    barcode_list = list(sorted_barcode_dict.keys())
    processed_barcodes = defaultdict(int)
    final_barcodes_with_mismatch = defaultdict(int)
    # Iterating over dictionary, complexity O(n)
    for _barcode, _count in sorted_barcode_dict.items():
        # Skip if barcode already seen before
        if _barcode in processed_barcodes.keys():
            continue
        processed_barcodes[_barcode] = 1
        final_barcodes_with_mismatch[_barcode] = _count
        # check current barcode with the rest
        # and skip already seen barcodes
        for _to_be_checked_barcode in barcode_list:
            # skip if already counted towards higher freq barcode
            if _to_be_checked_barcode in processed_barcodes:
                continue
            # count towards higher freq barcode
            if hamming_distance(_barcode, _to_be_checked_barcode) == 1:
                final_barcodes_with_mismatch[_barcode] += barcode_dict[_to_be_checked_barcode]
                # add to processed dictionary once checked
                processed_barcodes[_to_be_checked_barcode] = 1
    return final_barcodes_with_mismatch


def create_barcode_frequency_dict(fastq, forward_tag='CAT', reverse_tag='GTA'):
    """
    Create a dictionary with unique barcodes as keys,
    :param fastq:
    :param forward_tag:
    :param reverse_tag:
    :return:
    """
    # Initialize forward and reverse strand dictionaries
    barcode_counts_forward = defaultdict(int)
    barcode_loc_forward = defaultdict(list)
    barcode_counts_reverse = defaultdict(int)
    barcode_loc_reverse = defaultdict(list)
    for name, seq, quality, comment in fastq:
        # Get X and Y location for each read
        x_location, y_location = float(name.split(':')[2]), float(name.split(':')[3].split('#')[0])
        # Check if strand starts with forward or reverse strand
        if seq.startswith(forward_tag):
            # Assign next 8 bases as barcode
            _barcode = seq[3:11]
            # Add the counts and X and Y positions to the dictionary
            barcode_counts_forward[_barcode] += 1
            barcode_loc_forward[_barcode].append([x_location, y_location])
        elif seq.startswith(reverse_tag):
            _barcode = seq[3:11]
            # Get reverse complement of reverse strand (in order to simplify search and comparisons)
            _rev_complement_barcode = reverse_complement(_barcode)
            barcode_counts_reverse[_rev_complement_barcode] += 1
            barcode_loc_reverse[_barcode].append([x_location, y_location])
        else:
            raise Exception("Cannot determine the orientation of read.")
    barcode_counts_forward = iterate_over_dict(barcode_counts_forward)
    barcode_counts_reverse = iterate_over_dict(barcode_counts_reverse)
    return barcode_counts_forward, barcode_counts_reverse, barcode_loc_forward, barcode_loc_reverse


def barcodes_dict_from_fastq(fq_file, tags):
    """
    Read fastq file and creates two dictionaries of barcodes
    from forward and reverse reads, and two dictionaries with
    their respective x and y positions
    :param fq_file: input FastQ file
    :param tags: list containing forward and reverse strand identifiers.
    :return: 2 dictionaries of barcode frequencies from forward and reverse strands
        and 2 dictionaries of barcodes and their x and y positions
    """
    # Read fastq file
    if fq_file.endswith('.fq') or fq_file.endswith('.gz'):
        fastq_file = pyfastx.Fastx(fq_file, comment=True)
    else:
        # If file extension not `.fq` raise an error
        raise NotImplementedError("File format not supported")
    barcode_counts_fow, barcode_counts_rev, barcode_loc_fow, barcode_loc_rev = \
        create_barcode_frequency_dict(fastq_file, forward_tag=tags[0], reverse_tag=tags[1])
    return barcode_counts_fow, barcode_counts_rev, barcode_loc_fow, barcode_loc_rev


def summarize_barcodes(forward_dict, reverse_dict):
    """
    Summarize the frequencies of forward and reverse
    barcodes in a CSV, and sort them by their depth
    :param forward_dict: dictionary of forward barcodes and frequencies
    :param reverse_dict: dictionary of reverse barcodes and frequencies
    :return: CSV file containing the barcodes and their counts, sorted by depth of coverage
    """
    # Initialize the output dataframe
    output_data = pd.DataFrame(columns=['forward_barcode', 'forward_count', 'reverse_barcode', 'reverse_count'])
    # Iterate over forward dict keys to match with reverse dict keys
    for k, v in forward_dict.items():
        if k in reverse_dict.keys():
            row = {
                'forward_barcode': k,
                'forward_count': forward_dict[k],
                'reverse_barcode': reverse_complement(k),
                'reverse_count': reverse_dict[k]
            }
            output_data = pd.concat([output_data, pd.DataFrame([row])], ignore_index=True)
        # If barcode in forward dict but not in reverse dict
        else:
            row = {
                'forward_barcode': k,
                'forward_count': forward_dict[k],
                'reverse_barcode': None,
                'reverse_count': None
            }
            output_data = pd.concat([output_data, pd.DataFrame([row])], ignore_index=True)
    # Write all the frequencies that are in the reverse strand dictionary
    # but not in the forward strand dictionary
    # since reverse_dict contains reverse complements of barcodes as keys, we compare the keys directly
    rev_dict_keys = [i for i in list(reverse_dict.keys()) if i not in list(forward_dict.keys())]
    for k in rev_dict_keys:
        row = {
            'forward_barcode': None,
            'forward_count': None,
            'reverse_barcode': reverse_complement(k),
            'reverse_count': reverse_dict[k]
        }
        output_data = pd.concat([output_data, pd.DataFrame([row])], ignore_index=True)
    # Getting the minimum coverage between the forward and reverse strands
    output_data[['forward_count', 'reverse_count']] = \
        output_data[['forward_count', 'reverse_count']].apply(pd.to_numeric)
    output_data['min_count'] = output_data[['forward_count', 'reverse_count']].min(axis=1)
    # Sorting by minimum coverage
    output_data = output_data.sort_values(by='min_count')[['forward_barcode', 'forward_count',
                                                           'reverse_barcode', 'reverse_count']]
    return output_data


def additional_count_statistics(barcode_csv, output_path):
    """
    Get the counts of total forward and reverse barcodes
    and the spread of barcode counts for each barcode
    :param barcode_csv: pandas dataframe containing FWD/REV barcodes and their respective counts
    :param output_path: output path to store plots generated
    :return: Mean, variance and standard deviation of the counts
    """

    print(f"Total number of forward barcodes: {barcode_csv['forward_barcode'].nunique()}\n"
          f"Total count of forward barcodes: {barcode_csv['forward_count'].sum()}\n")
    print(f"Total number of reverse barcodes: {barcode_csv['reverse_barcode'].nunique()}\n"
          f"Total count of reverse barcodes: {barcode_csv['reverse_count'].sum()}\n\n")
    barcode_csv['total_reads'] = barcode_csv[['forward_count', 'reverse_count']].sum(axis=1)
    average_reads = np.mean(barcode_csv['total_reads'])
    standard_deviation = np.std(barcode_csv['total_reads'])
    variance = np.var(barcode_csv['total_reads'])
    print(f"Mean: {average_reads}\nStandard Deviation: {standard_deviation}")
    print(f"Variance: {variance}")
    # Plot a histogram showing distribution of the counts in both strands
    plt.figure(figsize=(10, 6))
    plt.hist(barcode_csv['forward_count'].dropna(), color='blue', bins=20, alpha=0.5, label='Forward counts')
    plt.hist(barcode_csv['reverse_count'].dropna(), color='green', bins=20, alpha=0.5, label='Reverse counts')
    plt.legend()
    plt.savefig(os.path.join(output_path, 'histograms_of_barcode_distributions.png'))
    plt.gcf()
    # Plot a bar plot showing counts per barcode
    plt.figure(figsize=(8, 5))
    # Randomly select 10 sample barcodes for ease of visualization
    sub_df = barcode_csv.dropna(subset=['forward_barcode']).sample(n=10)
    sub_df.plot(x='forward_barcode', y=['forward_count', 'reverse_count'], kind='bar', rot=45)
    plt.subplots_adjust(bottom=0.15)
    plt.savefig(os.path.join(output_path, 'barplot_difference_in_counts.png'))
    plt.gcf()


def main():

    parser = argparse.ArgumentParser(
        prog='barcode_analysis',
        description='Analyze the barcodes in a fastQ file and generate '
                    'additional statistics on the distributions of '
                    'forward and reverse strand barcodes')
    required_fastq = parser.add_argument_group('Required arguments')
    required_fastq.add_argument('--fastq_file', help='Input fastq file', required=True)
    parser.add_argument('-t', '--tags', nargs=2, default=['CAT', 'GTA'],
                        help='Tags specifying the forward and reverse strands (in that order) default: CAT, GTA')
    parser.add_argument('-o', '--output_path', help='Location of output file path')
    parser.add_argument('--additional_statistics', help='Calculate additional statistics to get distribution of counts'
                                                        'across barcodes',
                        action='store_true')
    # Calculate time taken to run the code
    parser.add_argument('--calculate_time', help='Calculate time taken to run the script',
                        action='store_true')

    args = parser.parse_args()
    print("Initiating script...\n\n")
    start = time.time()
    print("Creating barcode dictionary...\n\n")
    fow, rev, fow_loc, rev_loc = barcodes_dict_from_fastq(args.fastq_file, args.tags)
    print("Writing counts to csv file...\n\n")
    output_data = summarize_barcodes(fow, rev)
    if args.additional_statistics:
        print("Printing the counts and spread of the barcodes...\n\n")
        additional_count_statistics(output_data, args.output_path)
    output_data.to_csv(os.path.join(args.output_path, 'barcode_frequencies_ascending.csv'), index=False)
    print("\n\nComplete.\n")
    if args.calculate_time:
        print(f"Time taken to run the code: {round(time.time() - start, 2)} seconds")


if __name__ == '__main__':
    # Check for python v3.5+
    if not sys.version_info >= (3, 5):
        print("Please update your python to 3.5 or higher")
        exit()
    main()
