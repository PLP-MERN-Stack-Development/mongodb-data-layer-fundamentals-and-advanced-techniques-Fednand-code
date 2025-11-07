// queries.js - Complete MongoDB queries for all tasks
const { MongoClient } = require("mongodb");

// Connection URL
const uri = "mongodb://localhost:27017";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    // Connect to MongoDB
    await client.connect();
    console.log("✅ Connected successfully to MongoDB");

    // Use the plp_bookstore database
    const db = client.db("plp_bookstore");
    const booksCollection = db.collection("books");

    console.log("=".repeat(60));
    console.log("📚 MONGODB QUERIES - PLP BOOKSTORE");
    console.log("=".repeat(60));

    // =========================================================================
    // TASK 2: BASIC CRUD OPERATIONS
    // =========================================================================
    
    console.log("\n🎯 TASK 2: BASIC CRUD OPERATIONS");
    console.log("-".repeat(40));

    // 1. Find all books in a specific genre
    console.log("\n1. 📖 All Fantasy books:");
    const fantasyBooks = await booksCollection.find({ genre: "Fantasy" }).toArray();
    fantasyBooks.forEach(book => {
      console.log(`   - ${book.title} by ${book.author}`);
    });

    // 2. Find books published after a certain year
    console.log("\n2. 📅 Books published after 2000:");
    const recentBooks = await booksCollection.find({ published_year: { $gt: 2000 } }).toArray();
    recentBooks.forEach(book => {
      console.log(`   - ${book.title} (${book.published_year})`);
    });

    // 3. Find books by a specific author
    console.log("\n3. ✍️ Books by J.R.R. Tolkien:");
    const tolkienBooks = await booksCollection.find({ author: "J.R.R. Tolkien" }).toArray();
    tolkienBooks.forEach(book => {
      console.log(`   - ${book.title} (${book.published_year})`);
    });

    // 4. Update the price of a specific book
    console.log("\n4. 💰 Updating price of 'The Great Gatsby'...");
    const updateResult = await booksCollection.updateOne(
      { title: "The Great Gatsby" },
      { $set: { price: 15.99 } }
    );
    console.log(`   ✅ Modified ${updateResult.modifiedCount} document(s)`);

    // Verify the update
    const updatedBook = await booksCollection.findOne({ title: "The Great Gatsby" });
    console.log(`   New price: $${updatedBook.price}`);

    // 5. Delete a book by its title
    console.log("\n5. 🗑️ Deleting 'The Catcher in the Rye'...");
    const deleteResult = await booksCollection.deleteOne({ title: "The Catcher in the Rye" });
    console.log(`   ✅ Deleted ${deleteResult.deletedCount} document(s)`);

    // =========================================================================
    // TASK 3: ADVANCED QUERIES
    // =========================================================================
    
    console.log("\n🎯 TASK 3: ADVANCED QUERIES");
    console.log("-".repeat(40));

    // 1. Find books that are both in stock and published after 2010
    console.log("\n1. 📦 In-stock books published after 2010:");
    const inStockRecent = await booksCollection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    inStockRecent.forEach(book => {
      console.log(`   - ${book.title} (${book.published_year}) - $${book.price}`);
    });

    // 2. Using projection to return only title, author, and price fields
    console.log("\n2. 🎯 Books with projection (title, author, price only):");
    const projectedBooks = await booksCollection.find(
      { genre: "Fantasy" },
      { projection: { title: 1, author: 1, price: 1, _id: 0 } }
    ).toArray();
    console.log(projectedBooks);

    // 3. Sorting books by price (ascending and descending)
    console.log("\n3. 📊 Books sorted by price (ascending):");
    const sortedAsc = await booksCollection.find(
      {},
      { projection: { title: 1, price: 1, _id: 0 } }
    ).sort({ price: 1 }).limit(5).toArray();
    console.log(sortedAsc);

    console.log("\n   📊 Books sorted by price (descending):");
    const sortedDesc = await booksCollection.find(
      {},
      { projection: { title: 1, price: 1, _id: 0 } }
    ).sort({ price: -1 }).limit(5).toArray();
    console.log(sortedDesc);

    // 4. Pagination using limit and skip (5 books per page)
    console.log("\n4. 📄 Pagination - Page 1 (5 books):");
    const page1 = await booksCollection.find(
      {},
      { projection: { title: 1, author: 1, _id: 0 } }
    ).sort({ title: 1 }).limit(5).skip(0).toArray();
    console.log(page1);

    console.log("\n   📄 Pagination - Page 2 (5 books):");
    const page2 = await booksCollection.find(
      {},
      { projection: { title: 1, author: 1, _id: 0 } }
    ).sort({ title: 1 }).limit(5).skip(5).toArray();
    console.log(page2);

    // =========================================================================
    // TASK 4: AGGREGATION PIPELINE
    // =========================================================================
    
    console.log("\n🎯 TASK 4: AGGREGATION PIPELINE");
    console.log("-".repeat(40));

    // 1. Calculate average price by genre
    console.log("\n1. 📈 Average price by genre:");
    const avgPriceByGenre = await booksCollection.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" },
          bookCount: { $sum: 1 }
        }
      },
      {
        $sort: { averagePrice: -1 }
      }
    ]).toArray();
    console.log(avgPriceByGenre);

    // 2. Find author with the most books
    console.log("\n2. 👑 Author with most books:");
    const topAuthor = await booksCollection.aggregate([
      {
        $group: {
          _id: "$author",
          bookCount: { $sum: 1 }
        }
      },
      {
        $sort: { bookCount: -1 }
      },
      {
        $limit: 3
      }
    ]).toArray();
    console.log(topAuthor);

    // 3. Group books by publication decade
    console.log("\n3. 📅 Books by publication decade:");
    const booksByDecade = await booksCollection.aggregate([
      {
        $project: {
          title: 1,
          published_year: 1,
          decade: {
            $subtract: [
              "$published_year",
              { $mod: ["$published_year", 10] }
            ]
          }
        }
      },
      {
        $group: {
          _id: "$decade",
          bookCount: { $sum: 1 },
          books: { $push: "$title" }
        }
      },
      {
        $sort: { _id: 1 }
      }
    ]).toArray();
    console.log(booksByDecade);

    // =========================================================================
    // TASK 5: INDEXING
    // =========================================================================
    
    console.log("\n🎯 TASK 5: INDEXING");
    console.log("-".repeat(40));

    // 1. Create index on title field
    console.log("\n1. 🔍 Creating index on 'title' field...");
    await booksCollection.createIndex({ title: 1 });
    console.log("   ✅ Index created on title field");

    // 2. Create compound index on author and published_year
    console.log("\n2. 🔍 Creating compound index on 'author' and 'published_year'...");
    await booksCollection.createIndex({ author: 1, published_year: 1 });
    console.log("   ✅ Compound index created on author and published_year");

    // 3. Use explain() to demonstrate performance improvement
    console.log("\n3. ⚡ Performance comparison with explain():");
    
    // Without index (collection scan)
    console.log("\n   Without index (collection scan):");
    const withoutIndex = await booksCollection.find({ title: "The Hobbit" })
      .explain("executionStats");
    console.log(`   Documents examined: ${withoutIndex.executionStats.totalDocsExamined}`);
    console.log(`   Execution time: ${withoutIndex.executionStats.executionTimeMillis}ms`);

    // With index
    console.log("\n   With index (index scan):");
    const withIndex = await booksCollection.find({ title: "The Hobbit" })
      .hint({ title: 1 })
      .explain("executionStats");
    console.log(`   Documents examined: ${withIndex.executionStats.totalDocsExamined}`);
    console.log(`   Execution time: ${withIndex.executionStats.executionTimeMillis}ms`);

    // List all indexes
    console.log("\n4. 📋 Current indexes on books collection:");
    const indexes = await booksCollection.indexes();
    indexes.forEach((index, i) => {
      console.log(`   ${i + 1}. ${JSON.stringify(index.key)}`);
    });

  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    // Close the connection
    await client.close();
    console.log("\n🔌 Connection closed");
  }
}

// Run all queries
runQueries();