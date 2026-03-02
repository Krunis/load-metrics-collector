import { Client, Stream } from 'k6/net/grpc';
import { sleep } from 'k6';

export const options = {
    vus: 1,
    duration: '5s',
};

const client = new Client();
client.load(['/scripts'], 'server.proto');

export default function () {
    console.log(`🔵 Итерация ${__ITER} началась`);
    
    client.connect('host.docker.internal:8082', { 
        plaintext: true,
        timeout: '5s'
    });
    
    // Используем Promise, чтобы дождаться ответа
    const stream = new Stream(client, 'grpcapi.ServiceCollectorServer/SendMetric');
    
    let responseReceived = false;
    
    stream.on('data', (response) => {
        console.log(`📥 Получен ответ: acknowledged=${response.acknowledged}`);
        responseReceived = true;
    });
    
    stream.on('error', (error) => {
        console.error(`❌ Ошибка: ${error.message}`);
    });
    
    stream.on('end', () => {
        console.log('🏁 Стрим закрыт');
        client.close();
    });
    
    const message = {
        service: 'alpha',
        metric: 'cpu',
        value: 42.0,
        timestamp: Date.now()
    };
    
    console.log(`📤 Отправка: ${JSON.stringify(message)}`);
    stream.write(message);
    
    // Ждем ответа (но не бесконечно)
    let waited = 0;
    while (!responseReceived && waited < 10) {
        sleep(0.1);
        waited++;
    }
    
    if (responseReceived) {
        console.log(`✅ Итерация ${__ITER} завершена успешно`);
    } else {
        console.log(`⚠️ Итерация ${__ITER} завершена без ответа`);
    }
    
    // Важно: не закрываем стрим здесь!
    // stream.end(); // НЕ НУЖНО - хотим держать открытым
}