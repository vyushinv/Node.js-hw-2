const fs = require('fs');
const {
    randomInt
} = require('crypto');
const path = require('path');

const OUTPUT_DIR = './chunks'
const DELIMITER = ','
const COUNT = 33
const SIZE = 1000000

for (let index = 0; index < COUNT; index++) {

    let nums = []

    for (let index = 0; index < SIZE; index++) {
        nums.push(randomInt(1,100))
    }

    let ws = fs.createWriteStream(path.join(OUTPUT_DIR, `${index}.txt`))

    ws.write(nums.sort((a, b) => a - b).join(DELIMITER))

}