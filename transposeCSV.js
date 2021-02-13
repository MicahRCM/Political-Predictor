const fs = require('fs')
const { convertArrayToCSV } = require('convert-array-to-csv');
const converter = require('convert-array-to-csv');
const output_path = `./data/`

const transCSV = (subreddits, data, final = [], authors = []) => {

	// Remove this when running in production
    subreddits = ["askreddit", "politics", "iama"]
    let CSV_HEADERS = []
    // Dependent variable label to be in index 0 of final array
    CSV_HEADERS.push("Flair")

    // Generating label names. `_c` suffix denoting N comments, and `_s` suffix denoting sum of comment scores 
    for (let i = 0; i < subreddits.length; i++) {
        CSV_HEADERS.push(subreddits[i] += "_c")
        CSV_HEADERS.push(subreddits[i].slice(0, -2) + "_s")
    }
    // Remove this when running in production
    data = [{ "author": "bobby", "subreddit": "askreddit", "flair": "libleft", "count()": 2, "sum": 9 }, { "author": "chelsea", "subreddit": "iama", "flair": "libright", "count()": 1, "sum": 4 }, { "author": "bobby", "subreddit": "iama", "flair": "libleft", "count()": 1, "sum": 1 }, { "author": "chelsea", "subreddit": "askreddit", "flair": "libright", "count()": 1, "sum": 6 }]

    for (let i = 0; i < data.length; i++) {
        if (authors.indexOf(data[i]["author"]) < 0) {
            authors.push(data[i]["author"])
            final.push([data[i]["flair"]])
        }
        let c_index = CSV_HEADERS.indexOf(data[i]["subreddit"] + "_c")
        let s_index = CSV_HEADERS.indexOf(data[i]["subreddit"] + "_s")
        if (typeof(c_index) !== 'number' || typeof(s_index) !== 'number') {
        	console.error(`ERROR: c_index or s_index has an invalid index.\nc_index: ${c_index}, type: ${typeof(c_index)}\ns_index: ${s_index}, type: ${typeof(s_index)}`)
        }
        final[authors.indexOf(data[i]["author"])][c_index] = data[i]["count()"]
        final[authors.indexOf(data[i]["author"])][s_index] = data[i]["sum"]
    }
    // Add list of headers to final 2d array at index 0
    final.unshift(CSV_HEADERS)
    for (let i = 0; i < final.length; i++) {
    	final[i] = Array.from(final[i], item => item || 0)
    }
    fs.writeFileSync(output_path + "final.csv", convertArrayToCSV(final))
    console.log("Final written to", output_path + "final.csv")
}

transCSV()