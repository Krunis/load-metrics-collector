import { Client, Stream } from 'k6/net/grpc';
import { sleep } from 'k6';

export const options = {
    scenarios: {
        client_stream_test: {
            executor: 'constant-vus',
            vus: 50,              // 50 виртуальных пользователей
            duration: '1s',        // 1 секунда
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
    
    client.connect('host.docker.internal:8082', { plaintext: true });
    
    const stream = new Stream(client, 'grpcapi.ServiceCollectorServer/SendMetric');
    
    let finalResponse = null;
    
    stream.on('data', (response) => {
        finalResponse = response;
    });
    
    stream.on('error', (error) => {
        console.error(`❌ VU ${vuId} ошибка: ${error.message}`);
    });
    
    // Каждый VU отправляет ~111 сообщений * 9 комбинаций = 999 сообщений на VU
    // 50 VU * 1000 = 50 000 сообщений
    const messagesPerVU = 112; // 112 * 9 = 1008 сообщений на VU (чуть с запасом)
    let sentCount = 0;
    
    // Отправляем все сообщения максимально быстро
    for (let j = 0; j < messagesPerVU; j++) {
        const batchStart = Date.now();
        
        for (let i = 0; i < combinations.length; i++) {
            const combo = combinations[i];
            const value = randomInt(1, 150);
            
            const message = {
                service: combo.service,
                metric: combo.metric,
                value: parseFloat(value),
                timestamp: batchStart + i
            };
            
            stream.write(message);
            sentCount++;
        }
    }
    
    console.log(`📤 VU ${vuId} отправил ${sentCount} сообщений`);
    
    stream.end();
    
    // Ждем ответ
    let waited = 0;
    while (!finalResponse && waited < 50) {
        sleep(0.01);
        waited++;
    }
    
    client.close();
}