/* Julia just finished conducting a coding contest, and she needs your help assembling the leaderboard! Write a query to print the respective hacker_id and name of hackers who achieved full scores for more than one challenge. Order your output in descending order by the total number of challenges in which the acker earned a full score. If more than one hacker received full scores in same number of challenges, then sort them by ascending hacker_id */


select first.hacker_id, first.name
from (
       select Hackers.hacker_id, Hackers.name, count(Hackers.hacker_id) as num_challenges
       from Hackers join Submissions join Challenges join Difficulty
       on Hackers.hacker_id = Submissions.hacker_id
       where (Submissions.challenge_id = Challenges.challenge_id) & (Challenges.difficulty_level =        Difficulty.difficulty_level) & (Submissions.score = Difficulty.score)
       group by Hackers.hacker_id, Hackers.name
      ) as first
where num_challenges > 1
order by num_challenges desc, first.hacker_id asc