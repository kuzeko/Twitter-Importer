import random

def random_line(afile, max_chars = 123, min_chars = 5):
    line = next(afile)
    for num, aline in enumerate(afile):
        aline = aline.strip()
        if (len(aline) < min_chars or aline[0].islower() or len(aline) > max_chars) or random.randrange(num + 2):
            continue
        line = aline
    return line

def prepare_quote(text_file, signature=" -- Hamlet", max_chars = 123, min_chars = 5,):
    line = random_line(text_file, max_chars, min_chars)
    number = random.randrange(1,1000,2)
    line = "%d] " + line + signature
    return line