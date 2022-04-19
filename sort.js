var fs = require('fs');
const { rm }= require('fs/promises')
const { once } = require('events');
const readline = require('readline');
const { pipeline } = require('stream/promises');
var input = "temp_big_sentences_30mb.txt";
//var input = "temp_big_sentences.txt";
//var input = "input.txt";
//var input = "temp_big_sentences_2gb.txt";
var output = "output.txt";
var tmpFile = "tmp/tmp";
var highWaterMark = 1 * 1024 * 1024;
var files = 0;
var tmpFileNames = [];

var reader = fs.createReadStream(input, { highWaterMark: highWaterMark });
var writer = fs.createWriteStream(output, { highWaterMark: highWaterMark });
let unprocessed = "";
reader.on('data', (chunk) => {
    let chunkString = unprocessed + chunk.toString();
    unprocessed = '';
    let lines = [];
    let startIndex = 0;
    for (let ch = startIndex; ch < chunkString.length; ch++) {
        if (chunkString[ch] === '\n') {
            const line = chunkString.slice(startIndex, ch);
            lines.push(line);
            startIndex = ch + 1;
        }
    }
    if (chunkString[chunkString.length - 1] !== '\n') {
        unprocessed = chunkString.slice(startIndex)
    }
    lines.sort();
    let writeString = "";
    files++;
    tmpFileNames.push(tmpFile + files + ".txt");
    let tmpWriter = fs.createWriteStream(tmpFileNames[files - 1], { highWaterMark: highWaterMark });
    lines.forEach((str) => writeString = writeString + str + "\n");
    writeToFile(tmpWriter, writeString);
});

reader.on('end', async ()=>{
    await merge(tmpFileNames);
})

async function writeToFile(wr, str){
    await new Promise((resolve) =>
          wr.write(str, resolve)
        );
}

async function merge(tmpFileNames){
    const readers = tmpFileNames.map(
        name => readline.createInterface(
          { input: fs.createReadStream(name, { highWaterMark: highWaterMark }) }
        )[Symbol.asyncIterator]()
      )
    const values = await Promise.all(readers.map(r => r.next().then(e => e.value)));
    const valuesOrder = [];
    for(let i = 0; i < values.length; i++) valuesOrder.push([i, values[i]]);
    valuesOrder.sort(function(a, b) { return ('' + a[1]).localeCompare(b[1])});
    (async () => {
        while(readers.length > 0){
                    let fileOrder = valuesOrder[0][0];
                    if (typeof(await readers[fileOrder]) === 'undefined') {
                        valuesOrder.splice(fileOrder, 1);
                        readers.splice(fileOrder, 1);
                        break;
                    }
                    writeToFile(writer, valuesOrder[0][1] + "\n");
                    const nextLine = await readers[fileOrder].next();
                    if (!nextLine.done) {
                        valuesOrder[0] = [fileOrder, nextLine.value]
                    } else {
                        valuesOrder.splice(fileOrder, 1);
                        for(let t of valuesOrder) {
                            if(t[0] > fileOrder) t[0] = t[0] - 1
                        }
                        readers.splice(fileOrder, 1);
                    }
                    valuesOrder.sort(function(a, b) { return ('' + a[1]).localeCompare(b[1])});
            }
            showMemory();
            writer.end();
            console.log("finished");
            cleanUp(tmpFileNames);
        }

   )();

}

function cleanUp(tmpFileNames){
    return Promise.all(tmpFileNames.map(f => rm(f)));
}
function showMemory(){
    const used = process.memoryUsage();
    for (let key in used) {
      console.log(`${key} ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB`);
    }
}
