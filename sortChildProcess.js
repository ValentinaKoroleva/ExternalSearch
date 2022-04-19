var fs = require('fs');
    spawn = require('child_process').spawn,
    sort = spawn('sort', ['temp_big_sentences_300mb.txt']);

var writer = fs.createWriteStream('out.txt');

sort.stdout.on('data', function (data) {
  writer.write(data)
});

sort.on('exit', function (code) {
  if (code) console.log(code); //if some error
  writer.end();
});



