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
    client.connect('host.docker.internal:8082', { plaintext: true });
    
    const stream = new Stream(client, 'grpcapi.ServiceCollectorServer/SendMetric');
    
    let finalResponse = null;
    
    stream.on('data', (response) => {
        console.log(`✅ Получен финальный ответ`);
        finalResponse = response;
    });
    
    stream.on('error', (error) => {
        console.error(`❌ Ошибка: ${error.message}`);
    });
    
    // Каждый VU отправляет 111 сообщений (111 * 9 = 999 сообщений на VU)
    // При 10 VU = 9990 сообщений (почти 10000)
    const messagesPerVU = 111;
    
    for (let j = 0; j < messagesPerVU; j++) {
        const baseTime = Date.now() + (j * 100); // Разносим по времени
        
        for (let i = 0; i < combinations.length; i++) {
            const combo = combinations[i];
            const value = randomInt(1, 150);
            
            const message = {
                service: combo.service,
                metric: combo.metric,
                value: parseFloat(value),
                timestamp: baseTime + i
            };
            
            stream.write(message);
        }
    }
    
    console.log(`📤 VU отправил ${messagesPerVU * combinations.length} сообщений`);
    
    // Закрываем стрим - после этого сервер пришлет ответ
    stream.end();
    
    // Ждем ответ
    let waited = 0;
    while (!finalResponse && waited < 50) {
        sleep(0.1);
        waited++;
    }
    
    client.close();
    sleep(0.1);
}