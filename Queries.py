#Relevants posts that have been published by users who are not influencers
my_table = pd.read_sql("SELECT post.* FROM post INNER JOIN users ON post.username = users.username WHERE users.followers < 80000 ORDER BY post.n_like DESC ", con)
display(my_table)
#σ users . followers < 80000 (post ⋈ post . username = users . username users)

#Relevants posts that have been published by users who are not influencers
my_table = pd.read_sql("SELECT post.* FROM post LEFT JOIN influencers ON post.username = influencers.username WHERE influencers.username IS NULL ORDER BY post.n_like DESC", con)
display(my_table)
#τ post . n_like ↓(desc) π post  σ influencers . username = NULL (post ⋈ post . username = influencers . username influencers)

#- Number of relevant posts from each advertising company in ascending order > 27
my_table = pd.read_sql("SELECT DISTINCT id_company , COUNT () FROM post GROUP BY id_company HAVING count()>25 ORDER BY count DESC", con)
display(my_table)


#-	Locations with more than 5M interactions and with more than 5 relevant posts
my_table = pd.read_sql("SELECT location.* FROM location JOIN (SELECT city, COUNT() AS post_count FROM post GROUP BY city HAVING COUNT() > 5) post ON location.city = post.city GROUP BY location.city HAVING SUM(location.interactions) > 8000000 ORDER BY location.interactions DESC",con)
display(my_table)
#π location.* (σ SUM(interactions) > 8000000 (location ⨝ city = city ((π city, COUNT(*) (σ COUNT(*) > 5 (ρ city (post)))))))


# Post with more than 300,000 likes and more than 10,000 comments, Games type and located in Lyon. Limited
my_table = pd.read_sql("SELECT * FROM post WHERE n_like>300000 AND n_comments>10000 AND city='Lyon' AND type='Games' ORDER BY n_comments ASC OFFSET 0 LIMIT 5",con)
display(my_table)
# π * (σ n_like > 300000 ∧ n_comments > 10000 ∧ city = 'Lyon' ∧ type = 'Games' (post))


# Influencers with more than 1000,000 followers, whose broadcast channel has less than 30,000 users and who do not have any relevant posts
my_table = pd.read_sql("SELECT users.*, post.id_post, channels.name_channels, channels.n_users FROM influencers LEFT JOIN users ON influencers.username = users.username LEFT JOIN post ON influencers.username = post.username LEFT JOIN channels ON influencers.username = channels.username WHERE users.followers > 1000000 AND post.username IS NULL AND channels.n_users >30000",con)
display(my_table)

# TOP 10 post with more likes and comments by users with 24 years or less
