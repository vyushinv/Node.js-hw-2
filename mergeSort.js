const fs = require('fs');
const path = require('path');

const INPUT_DIR = './parts'
const OUTPUT_PATH = './result.txt'
const DELIMITER = ','
const STREAM_BUFFER_SIZE = 16
const STAT_INTERVAL = 10


const streams = []
fs.readdirSync(INPUT_DIR).forEach(file => {
    streams.push(fs.createReadStream(path.join(INPUT_DIR, file), {
        encoding: 'utf8',
        highWaterMark: STREAM_BUFFER_SIZE * 1024
    }))
})

let ws = fs.createWriteStream(OUTPUT_PATH)

let active = streams.length
let buffer = []
let ready = 0
let bufferTemp = Array(active).fill('')

streams.forEach((stream, id) => {

    stream.on('data', (chunk) => {
        stream.pause()

        chunk = bufferTemp[id] + chunk
        bufferTemp[id] = ''

        let temp = chunk.split(DELIMITER)
        let lastChar = chunk.slice(-1)

        if (lastChar === DELIMITER) {
            temp.pop()
        } else {
            bufferTemp[id] = temp.pop()
        }

        buffer.push({id: id,chunk: temp})

        if (++ready === active) {
            mergeSort()
        }

    }).on('end', () => {
        buffer.push({id: id,chunk: [bufferTemp[id]] })
        mergeSort()
    })

})

function bufferIsFill() {
    return (buffer.length > 0 && buffer.every(el => el.chunk.length > 0))
}

function mergeSort() {

    if (bufferIsFill()) {
        let streamId
        let bufferId
        let result = ''

        while (bufferIsFill()) {

            const minOfColumn = buffer.map(x => x.chunk[0]).sort((a, b) => a - b)[0]
            streamId = buffer.filter(el => el.chunk[0] === minOfColumn)[0].id
            bufferId = buffer.findIndex(el => el.id === streamId)
            const shift = buffer[bufferId].chunk.shift()
            result = result + DELIMITER + shift

        }
        
        ws.write(result)
        buffer.splice(bufferId, 1)

        if (streams[streamId].readableEnded) {
            mergeSort()
        } else {
            --ready
            streams[streamId].resume()
        }
    }
}

var stat = setInterval(() => {
    if (active===0) {
        clearInterval(stat)
    } 
    console.log(`Total memory allocated (rss): ${Math.round(process.memoryUsage().rss / 1024 / 1024 * 100) / 100}, heap: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024 * 100) / 100} `)
}, STAT_INTERVAL * 1000)