import * as zookeeper from 'node-zookeeper-client';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import _ from 'lodash';
const argv = await yargs(hideBin(process.argv))
    .option('zk', {
    alias: 'zookeeper',
    type: 'string',
    description: 'Zookeeper 连接串',
    default: '70.36.96.27:2181',
    demandOption: false,
})
    .option('path', {
    alias: 'barrierPath',
    type: 'string',
    description: '屏障节点路径',
    default: '/barrier',
    demandOption: false,
})
    .option('count', {
    alias: 'participantCount',
    type: 'number',
    description: '需要同步的进程数量',
    default: 50,
    demandOption: false,
})
    .option('value', {
    alias: 'participantValue',
    type: 'number',
    description: '参与者数值',
    demandOption: false,
})
    .help()
    .argv;
const zkConnectionString = argv.zk;
const barrierPath = argv.path;
const participantCount = argv.count;
const participantValue = argv.value;
const client = zookeeper.createClient(zkConnectionString);
process.on('SIGINT', async () => {
    // Ctrl+C
    console.log('SIGINT: 用户中断');
    client.close();
    process.exit();
});
process.on('SIGTERM', async () => {
    // timeout docker-compose down/stop 会触发 SIGTERM 信号
    console.log('SIGTERM: 终止请求');
    client.close();
    process.exit();
});
// 屏障同步逻辑，统计数值
function enterBarrier(client, barrierPath, participantCount, participantValue) {
    return new Promise((resolve, reject) => {
        // 记录屏障开始时间
        const startTime = Date.now();
        // 创建屏障父节点（如不存在）
        client.mkdirp(barrierPath, (err) => {
            if (err)
                return reject(err);
            // 每个参与者创建自己的临时子节点
            const nodePath = `${barrierPath}/participant-`;
            client.create(nodePath, participantValue && Buffer.from(participantValue.toString()), zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, (err, createdPath) => {
                if (err)
                    return reject(err);
                let lastChildren = [];
                function checkBarrier() {
                    client.getChildren(barrierPath, (event) => {
                        // 节点变更时再次检查
                        checkBarrier();
                    }, async (err, children) => {
                        if (err)
                            return reject(err);
                        if (participantValue != null) {
                            const added = children.filter(child => !lastChildren.includes(child));
                            lastChildren = children;
                            if (added.length > 0) {
                                const startTime = Date.now();
                                // 并发获取所有节点的数值
                                const allValues = await Promise.all(children.map(child => {
                                    const fullPath = `${barrierPath}/${child}`;
                                    return new Promise(resolve => {
                                        client.getData(fullPath, (err, data) => {
                                            resolve(Number(data.toString()));
                                        });
                                    });
                                }));
                                // 统计最大、最小、平均值
                                const max = _.max(allValues);
                                const min = _.min(allValues);
                                const avg = _.mean(allValues);
                                console.log('当前节点总数:', allValues.length);
                                console.log(`最大值: ${max}, 最小值: ${min}, 平均值: ${avg}`);
                                // 打印新增节点及其数值
                                for (const child of added) {
                                    const idx = children.indexOf(child);
                                    console.log('新增节点 id:', child, '数值:', allValues[idx]);
                                }
                                console.log(`耗时: ${((Date.now() - startTime) / 1000).toFixed(1)} 秒`);
                            }
                        }
                        if (children.length >= participantCount) {
                            console.log('屏障已通过！所有参与者已就绪。');
                            console.log(`屏障耗时: ${((Date.now() - startTime) / 1000).toFixed(1)} 秒`);
                            resolve();
                        }
                        else {
                            console.log(barrierPath, `等待中，当前已就绪: ${children.length} / ${participantCount}`);
                        }
                    });
                }
                checkBarrier();
            });
        });
    });
}
client.once('connected', async () => {
    try {
        await enterBarrier(client, barrierPath, participantCount, participantValue);
        console.log('屏障已通过，等待所有参与者检测到屏障...');
        setTimeout(() => {
            console.log('安全退出');
            client.close();
            process.exit();
        }, 2000);
    }
    catch (e) {
        console.error('屏障出错:', e);
        client.close();
        process.exit(1);
    }
});
client.connect();
//# sourceMappingURL=distributed_barrier.js.map
