import { Client, Stream } from 'k6/net/grpc';
import { sleep } from 'k6';

export const options = {
    scenarios: {
        client_stream_test: {
            executor: 'constant-vus',
            vus: 10,
            duration: '1s',
        },
    },
};

const services = ['alpha', 'beta', 'gamma'];
const metrics = ['cpu', 'memory', 'disk'];

const combinations = [];
for (const service of services) {
    for (const metric of metrics) {
        combinations.push({ service, metric });
    }
}

function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

const client = new Client();
client.load(['/scripts'], 'server.proto');

export default function () {
    const vuId = __VU;
    const startTime = Date.now();
    
    client.connect('host.docker.internal:8082', { plaintext: true });
    
    const stream = new Stream(client, 'grpcapi.ServiceCollectorServer/SendMetric');
    
    let finalResponse = null;
    
    stream.on('data', (response) => {
        console.log(`✅ VU ${vuId} получил ответ`);
        finalResponse = response;
    });
    
    stream.on('error', (error) => {
        console.error(`❌ VU ${vuId} ошибка: ${error.message}`);
    });
    
    // 111 сообщений на VU = 999 сообщений на VU (111 * 9)
    const messagesPerVU = 111;
    let sentCount = 0;
    
    for (let j = 0; j < messagesPerVU; j++) {
        const batchStart = Date.now();
        
        for (let i = 0; i < combinations.length; i++) {
            const combo = combinations[i];
            const value = randomInt(1, 150);
            
            const message = {
                service: combo.service,
                metric: combo.metric,
                value: parseFloat(value),
                timestamp: batchStart + i // микро-сдвиг для уникальности
            };
            
            stream.write(message);
            sentCount++;
        }
        
        // Контролируем скорость, чтобы уложиться в 1 секунду
        const elapsed = Date.now() - startTime;
        if (elapsed > 950) { // Если почти вышли за секунду
            break;
        }
    }
    
    console.log(`📤 VU ${vuId} отправил ${sentCount} сообщений за ${Date.now() - startTime}ms`);
    
    stream.end();
    
    // Ждем ответ
    let waited = 0;
    while (!finalResponse && waited < 50) {
        sleep(0.01); // 10ms
        waited++;
    }
    
    client.close();
}