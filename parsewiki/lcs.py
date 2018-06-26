def text_equal(token1, token2):
    return token1[0] == token2[0]

class Edit:
    UNCHANGED = 0
    ADDED = 1
    REMOVED = 2

    
class EditBuffer:
    def __init__(self, action=None):
        self.action = action
        self.edits = []

    def edit_tuple(self):
        if self.action == Edit.UNCHANGED:
            type_ = "UNC"
        if self.action == Edit.ADDED:
            type_ = "INS"
        if self.action == Edit.REMOVED:
            type_ = "DEL"
        return type_, tuple(self.edits)
        
    def __str__(self):
        type_, edits = self.edit_tuple()
        return "{}: {}".format(type_, edits)


def lcs(seq1, seq2, equal_function=lambda x,y: x == y):    
    len1 = len(seq1)
    len2 = len(seq2)
    # init table
    table = []
    for i1 in range(0, len1 + 1):
        table.append([0] * (len2 + 1))
    ind1 = 0
    while ind1 < len1:
        ind2 = 0
        while ind2 < len2:
            if equal_function(seq1[ind1], seq2[ind2]):
                table[ind1 + 1][ind2 + 1] = table[ind1][ind2] + 1
            else:
                table[ind1 + 1][ind2 + 1] = max(table[ind1 + 1][ind2],
                                                table[ind1][ind2 + 1])
            ind2 += 1
        ind1 += 1
    table.pop(1)
    return table

def diff(fseq1, fseq2):
    """
    Adapted from:
    en.wikipedia.org/wiki/Longest_common_subsequence_problem#Print_the_diff
    """
    # insert common token at start, so lcs
    # can be used as a diff
    seq1 = list(fseq1).copy()
    seq2 = list(fseq2).copy()
    CMARK = ("MARK", 0)
    seq1.insert(0, CMARK)
    seq2.insert(0, CMARK)
    seq1 = tuple(seq1)
    seq2 = tuple(seq2)

    
    table = lcs(seq1, seq2, text_equal)
    ind1 = len(table) - 1
    ind2 = len(table[0]) - 1

    buffer = EditBuffer()
    last_edit = None
    last_action = None
    while ind1 > 0  or ind2 > 0:        
        if ind1 > 0 and ind2 > 0 and\
           text_equal(seq1[ind1 - 1], seq2[ind2 - 1]):
            last_action = Edit.UNCHANGED
            last_edit = seq1[ind1 - 1]
            ind1 -= 1
            ind2 -= 1
        elif ind2 > 0 and\
             (ind1 == 0 or table[ind1][ind2 - 1] >= table[ind1 - 1][ind2]):
            last_action = Edit.ADDED
            last_edit = seq2[ind2 - 1]
            ind2 -= 1
        elif ind1 > 0 and\
             (ind2 == 0 or table[ind1][ind2 - 1] < table[ind1 - 1][ind2]):
            last_action = Edit.REMOVED
            last_edit = seq1[ind1 - 1]
            ind1 -= 1

        # just for first iteration
        if buffer.action is None:
            buffer.action = last_action

        if buffer.action != last_action:
            type_, edits = buffer.edit_tuple()
            if buffer.action != Edit.UNCHANGED:
                yield type_, last_edit[1], edits
            # insert only the text
            buffer.edits = [last_edit[0]]
            buffer.action = last_action
        else:
            buffer.edits.insert(0, last_edit[0])

    type_, edits = buffer.edit_tuple()
    if buffer.action != Edit.UNCHANGED:
        return type_, last_edit[1], edits
