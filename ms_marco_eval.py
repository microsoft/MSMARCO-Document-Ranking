"""
This module computes evaluation metrics for MSMARCO dataset on the ranking task. Intenral hard coded eval files version. DO NOT PUBLISH!
Command line:
python msmarco_eval_ranking.py <path_to_candidate_file>

Creation Date : 06/12/2018
Last Modified : 08/06/2020
Authors : Daniel Campos <dacamp@microsoft.com>, Rutger van Haasteren <ruvanh@microsoft.com>
"""
import sys
import os
import statistics

from collections import Counter

MaxMRRRank = 100

def load_reference_from_stream(f):
    """Load Reference reference relevant document
    Args:f (stream): stream to load.
    Returns:qids_to_relevant_documentids (dict): dictionary mapping from query_id (int) to relevant document (list of ints). 
    """
    qids_to_relevant_documentids = {}
    for l in f:
        try:
            l = l.strip().split('\t')
            qid = int(l[0])
            if qid in qids_to_relevant_documentids:
                pass
            else:
                qids_to_relevant_documentids[qid] = []
            qids_to_relevant_documentids[qid].append(l[2])
        except:
            raise IOError('\"%s\" is not valid format' % l)
    return qids_to_relevant_documentids

def load_reference(path_to_reference):
    """Load Reference reference relevant document
    Args:path_to_reference (str): path to a file to load.
    Returns:qids_to_relevant_documentids (dict): dictionary mapping from query_id (int) to relevant documents (list of ints). 
    """
    with open(path_to_reference,'r') as f:
        qids_to_relevant_documentids = load_reference_from_stream(f)
    return qids_to_relevant_documentids

def validate_candidate_has_enough_ranking(qid_to_ranked_candidate_documents):
    for qid in qid_to_ranked_candidate_documents:
        if len(qid_to_ranked_candidate_documents[qid]) != MaxMRRRank:
            print('Not enough documents ranked. Please Provide top 100 documents for qid:{}'.format(qid))

def load_candidate_from_stream(f):
    """Load candidate data from a stream.
    Args:f (stream): stream to load.
    Returns:qid_to_ranked_candidate_documents (dict): dictionary mapping from query_id (int) to a list of 1000 document ids(int) ranked by relevance and importance
    """
    qid_to_ranked_candidate_documents = {}
    for l in f:
        try:
            l = l.strip().split('\t')
            qid = int(l[0])
            did = l[1]
            rank = int(l[2])
            if qid in qid_to_ranked_candidate_documents:
                pass    
            else:
                # By default, all PIDs in the list of 1000 are 0. Only override those that are given
                qid_to_ranked_candidate_documents[qid] = []
            qid_to_ranked_candidate_documents[qid].append((did,rank))
        except:
            raise IOError('\"%s\" is not valid format' % l)
    validate_candidate_has_enough_ranking(qid_to_ranked_candidate_documents)
    print('Quantity of Documents ranked for each query is as expected. Evaluating')
    return {qid: sorted(qid_to_ranked_candidate_documents[qid], key=lambda x:(x[1], x[0]), reverse=False) for qid in qid_to_ranked_candidate_documents}         
def load_candidate(path_to_candidate):
    """Load candidate data from a file.
    Args:path_to_candidate (str): path to file to load.
    Returns:qid_to_ranked_candidate_documents (dict): dictionary mapping from query_id (int) to a list of 1000 document ids(int) ranked by relevance and importance
    """
    
    with open(path_to_candidate,'r') as f:
        qid_to_ranked_candidate_documents = load_candidate_from_stream(f)
    return qid_to_ranked_candidate_documents

def quality_checks_qids(qids_to_relevant_documentids, qids_to_ranked_candidate_documents):
    """Perform quality checks on the dictionaries

    Args:
    p_qids_to_relevant_documentids (dict): dictionary of query-document mapping
        Dict as read in with load_reference or load_reference_from_stream
    p_qids_to_ranked_candidate_documents (dict): dictionary of query-document candidates
    Returns:
        bool,str: Boolean whether allowed, message to be shown in case of a problem
    """
    message = ''
    allowed = True

    # Create sets of the QIDs for the submitted and reference queries
    candidate_set = set(qids_to_ranked_candidate_documents.keys())
    ref_set = set(qids_to_relevant_documentids.keys())

    # Check that we do not have multiple documents per query
    for qid in qids_to_ranked_candidate_documents:
        # Remove all zeros from the candidates
        duplicate_pids = set([item for item, count in Counter(qids_to_ranked_candidate_documents[qid]).items() if count > 1])

        if len(duplicate_pids-set([0])) > 0:
            message = "Cannot rank a document multiple times for a single query. QID={qid}, PID={pid}".format(
                    qid=qid, pid=list(duplicate_pids)[0])
            allowed = False

    return allowed, message

def compute_metrics(qids_to_relevant_documentids, qids_to_ranked_candidate_documents, exclude_qids):
    """Compute MRR metric
    Args:    
    p_qids_to_relevant_documentids (dict): dictionary of query-document mapping
        Dict as read in with load_reference or load_reference_from_stream
    p_qids_to_ranked_candidate_documents (dict): dictionary of query-document candidates
    Returns:
        dict: dictionary of metrics {'MRR': <MRR Score>}
    """
    all_scores = {}
    MRR = 0
    qids_with_relevant_documents = 0
    ranking = []
    
    for qid in qids_to_ranked_candidate_documents:
        if qid in qids_to_relevant_documentids and qid not in exclude_qids:
            ranking.append(0)
            target_pid = qids_to_relevant_documentids[qid]
            candidate_pid = qids_to_ranked_candidate_documents[qid]
            for i in range(0,MaxMRRRank):
                if candidate_pid[i][0] in target_pid:
                    MRR += 1/(i + 1)
                    ranking.pop()
                    ranking.append(i+1)
                    break
    if len(ranking) == 0:
        raise IOError("No matching QIDs found. Are you sure you are scoring the evaluation set?")
    
    MRR = MRR/len(qids_to_relevant_documentids)
    all_scores['MRR @100'] = MRR
    all_scores['QueriesRanked'] = len(set(qids_to_ranked_candidate_documents)-exclude_qids)
    return all_scores
                
def compute_metrics_from_files(path_to_reference, path_to_candidate, exclude_qids, perform_checks=True):
    """Compute MRR metric
    Args:    
    p_path_to_reference_file (str): path to reference file.
        Reference file should contain lines in the following format:
            QUERYID\tdocumentID
            Where documentID is a relevant document for a query. Note QUERYID can repeat on different lines with different documentIDs
    p_path_to_candidate_file (str): path to candidate file.
        Candidate file sould contain lines in the following format:
            QUERYID\tdocumentID1\tRank
            If a user wishes to use the TREC format please run the script with a -t flag at the end. If this flag is used the expected format is 
            QUERYID\tITER\tDOCNO\tRANK\tSIM\tRUNID 
            Where the values are separated by tabs and ranked in order of relevance 
    Returns:
        dict: dictionary of metrics {'MRR': <MRR Score>}
    """
    
    qids_to_relevant_documentids = load_reference(path_to_reference)
    qids_to_ranked_candidate_documents = load_candidate(path_to_candidate)
    if perform_checks:
        allowed, message = quality_checks_qids(qids_to_relevant_documentids, qids_to_ranked_candidate_documents)
        if message != '': print(message)

    return compute_metrics(qids_to_relevant_documentids, qids_to_ranked_candidate_documents, exclude_qids)
def load_exclude(path_to_exclude_folder):
    """Load QIDS for queries to exclude
    Args: 
    path_to_exclude_folder (str): path to folder where exclude files are located

    Returns: 
        set: a set with all qid's to exclude
    """
    qids = set()
    # List all files in a directory using os.listdir
    for a_file in os.listdir(path_to_exclude_folder):
        if os.path.isfile(os.path.join(path_to_exclude_folder, a_file)):
            with open(os.path.join(path_to_exclude_folder, a_file), 'r') as f:
                f.readline() #header
                for l in f:
                    qids.add(int(l.split('\t')[0]))
    print("{} excluded qids loaded".format(len(qids)))
    return qids

def main():
    """Command line:
    python document_ranking.py <path_to_candidate_file> <path_to_reference_file> <queries_to_exclude>
    """
    if len(sys.argv) == 1:
        #print("Usage:  document_ranking.py <path_to_candidate_file> <path_to_reference_file> <queries_to_exclude>") for public version
        print("Usage:  document_ranking.py <path_to_candidate_file> ")
    else:
        if len(sys.argv) == 3:
            exclude_qids = set()
        elif len(sys.argv) == 1:
            exclude_qids = load_exclude(sys.argv[3]) #Public implementation
        exclude_qids = load_exclude('exclude/')
        path_to_candidate = sys.argv[1]
        path_to_reference = 'docleaderboard-qrels.tsv'
        metrics = compute_metrics_from_files(path_to_reference, path_to_candidate, exclude_qids)
        print('#####################')
        for metric in sorted(metrics):
            print('{}: {}'.format(metric, metrics[metric]))
        print('#####################')

if __name__ == '__main__':
    main()
