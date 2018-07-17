import difflib

def diff(fseq1, fseq2):
    """Use difflib to compute differences of two
    sequences of strings."""
    differ = difflib.SequenceMatcher(a=fseq1, b=fseq2, autojunk = False)
    return differ.get_opcodes()

def token_diff(seq1, seq2):
    """Given a list of composite
    elements where the first component is a string and
    the second component is a position associated,
    return the diff of the two sequences based
    on the diff of the strings in the first component.

    Eg:
      seq1 = [("Trento", 0), ("is", 10), ("a", 2), ("city", 3)]
      seq2 = [("Trento", 0), ("is", 1), ("the", 2), ("city", 3)]
    
    The function returns the diff based on the diff of the
    strings in the first components.

    ("DEL", 10, "a"), ("INS", 10, "the")
    """
    words_seq1 = [word for word,_ in seq1]
    words_seq2 = [word for word,_ in seq2]

    # sequences a and b.
    # ahi => sequence a, higher matching part
    # blo => sequence b, lower matchin part
    # tag => the operation required to transfor sequence a in b
    #        either "equal", "insert", "replace" or "delete"
    lseq1 = len(words_seq1)
    lseq2 = len(words_seq2)
    for tag, alo, ahi, blo, bhi in diff(words_seq1, words_seq2):
        if alo >= lseq1:
            alo = lseq1 - 1
        if blo >= lseq2:
            blo = lseq2 - 1
        if tag == "replace":
            yield "DEL", seq1[alo][1], words_seq1[alo:ahi]
            yield "INS", seq1[alo][1], words_seq2[blo:bhi]
        elif tag == "insert":
            yield "INS", seq1[alo][1], words_seq2[blo:bhi]
        elif tag == "delete":
            yield "DEL", seq1[alo][1], words_seq1[alo:ahi]
