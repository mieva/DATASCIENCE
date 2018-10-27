def minion_game(word):
    vowels = ["A","E","I","O","U"]
    kevin_score = 0
    stuart_score = 0
    
    for i in range(len(word)):
        sub_word = word[i:]
        if sub_word[0] in vowels:
            kevin_score += len(sub_word)
        else:
            stuart_score += len(sub_word)
     # Print results
    if kevin_score > stuart_score:
        print("Kevin", kevin_score)
    elif kevin_score < stuart_score:
        print("Stuart", stuart_score)
    else:
        print("DRAW")