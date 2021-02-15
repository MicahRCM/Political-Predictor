const fs = require('fs')
const { convertArrayToCSV } = require('convert-array-to-csv');
const converter = require('convert-array-to-csv');
const output_path = `./`
const { Pool, Client } = require('pg');
const { mainModule } = require('process');


// psql things
const poolConnection = new Pool({
    connectionString: "postgres://grover:pass1@localhost/gzp_data"
});

const SUBREDDIT_DISPERSION_THRESHOLD = 50



let getAllViableSubreddits = async () => {
    const query = `
     select subreddit
            from ppm_flair_prediction
            group by subreddit
            having count(*) >= ${SUBREDDIT_DISPERSION_THRESHOLD}
    `
    let res = await poolConnection.query(query)
    return res.rows.map((obj) => obj.subreddit)
}

let getFilteredFlairPredictionData = async () => {
    const query = `
        select * 
        from ppm_flair_prediction
        where author in (
            select author
            from 
            (select author,flair, count(*)
                from ppm_flair_prediction
                group by author, flair) as t1
            group by author
            having count(*) = 1
        ) and subreddit in (
            select subreddit
            from ppm_flair_prediction
            group by subreddit
            having count(*) >= ${SUBREDDIT_DISPERSION_THRESHOLD}
        );
    `
    let res = await poolConnection.query(query)
    return res.rows
}

const transCSV = (subreddits, data, final = [], authors = []) => {

	// Remove this when running in production
    // subreddits = ["askreddit", "politics", "iama"]
    let CSV_HEADERS = []
    // Dependent variable label to be in index 0 of final array
    CSV_HEADERS.push("Flair")

    // Generating label names. `_c` suffix denoting N comments, and `_s` suffix denoting sum of comment scores 
    for (let i = 0; i < subreddits.length; i++) {
        CSV_HEADERS.push(subreddits[i] += "_c")
        CSV_HEADERS.push(subreddits[i].slice(0, -2) + "_s")
    }
    // Remove this when running in production
    // data = [{ "author": "bobby", "subreddit": "askreddit", "flair": "libleft", "posts": 2, "sum": 9 }, { "author": "chelsea", "subreddit": "iama", "flair": "libright", "posts": 1, "sum": 4 }, { "author": "bobby", "subreddit": "iama", "flair": "libleft", "posts": 1, "sum": 1 }, { "author": "chelsea", "subreddit": "askreddit", "flair": "libright", "posts": 1, "sum": 6 }]

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
        final[authors.indexOf(data[i]["author"])][c_index] = data[i]["posts"]
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


async function main(){
    let data = await getFilteredFlairPredictionData()
    console.log(data)
    let allViableSubreddits = await getAllViableSubreddits()
    console.log(allViableSubreddits)
    transCSV(allViableSubreddits,data)
}

main()