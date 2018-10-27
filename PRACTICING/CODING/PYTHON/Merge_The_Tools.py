def merge_the_tools(string, k):
    
    from collections import OrderedDict

    chunks = len(string)//k
    chunk_size = k
    
    # Split according requirements
    for i in range(0, chunks*chunk_size, chunk_size):
        word = string[i:i+chunk_size]
        
        # Delete non distinct characters
        norepeat = "".join(OrderedDict.fromkeys(word))
        print(norepeat)
    