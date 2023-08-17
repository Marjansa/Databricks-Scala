%scala
// in this example, we're working with a list of words and performing the following tasks using lambda functions:

// Task 1: Count the number of characters in each word:
// We use the map transformation to create a new RDD with pairs of words and their corresponding lengths.

// Task 2: Filter words that start with the letter "a":
// We use the filter transformation to keep only the words that start with the letter "a".

// Task 3: Find the total length of all words:
// We use the map transformation to extract the lengths from the wordLengthsRDD, and then use the reduce transformation to calculate the sum of lengths.

// Task 4: Create a comma-separated list of words:
// We use the reduce transformation to concatenate all words with commas in between.

// Each task showcases different ways to use lambda functions for various transformations on RDDs. Lambda functions provide a concise way to express these transformations in Spark, making your code more readable and expressive.
// Create an RDD with a list of words
val wordsRDD = sc.parallelize(List("apple", "banana", "cherry", "date", "elderberry"))

// Task 1: Count the number of characters in each word
val wordLengthsRDD = wordsRDD.map(word => (word, word.length))

// Task 2: Filter words that start with the letter "a"
val wordsStartingWithARDD = wordsRDD.filter(word => word.startsWith("a"))

// Task 3: Find the total length of all words
val totalLength = wordLengthsRDD.map(pair => pair._2).reduce((x, y) => x + y)

// Task 4: Create a comma-separated list of words
val commaSeparatedList = wordsRDD.reduce((x, y) => s"$x, $y")

// Print the results
println("Word Lengths: " + wordLengthsRDD.collect().mkString(", "))
println("Words Starting with 'a': " + wordsStartingWithARDD.collect().mkString(", "))
println("Total Length of All Words: " + totalLength)
println("Comma-Separated List of Words: " + commaSeparatedList)
