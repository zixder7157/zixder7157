import * as zookeeper from 'node-zookeeper-client';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import axios from 'axios';
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
    .help()
    .argv;
const zkConnectionString = argv.zk;
const barrierPath = argv.path;
const participantCount = argv.count;
const client = zookeeper.createClient(zkConnectionString);
// 优雅退出
process.on('SIGINT', async () => {
    console.log('SIGINT: 用户中断');
    client.close();
    process.exit();
});
process.on('SIGTERM', async () => {
    console.log('SIGTERM: 终止请求');
    client.close();
    process.exit();
});
// 获取自身外网IP
async function getPublicIP() {
    try {
        const res = await axios.get('https://api.ipify.org');
        return res.data;
    }
    catch (e) {
        console.error('获取外网IP失败:', e.message);
        return 'unknown';
    }
}
// 屏障同步逻辑，统计IP
function enterBarrier(client, barrierPath, participantCount) {
    return new Promise(async (resolve, reject) => {
        // 创建屏障父节点（如不存在）
        client.mkdirp(barrierPath, async (err) => {
            if (err)
                return reject(err);
            // 获取自身IP，作为节点数据
            const ip = await getPublicIP();
            const nodePath = `${barrierPath}/participant-`;
            client.create(nodePath, Buffer.from(ip), zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, (err, createdPath) => {
                if (err)
                    return reject(err);
                let lastChildren = [];
                async function checkBarrier() {
                    client.getChildren(barrierPath, (event) => {
                        // 节点变更时再次检查
                        checkBarrier();
                    }, async (err, children) => {
                        if (err)
                            return reject(err);
                        // 新增节点
                        const added = children.filter(child => !lastChildren.includes(child));
                        lastChildren = children;
                        if (added.length > 0) {
                            // 并发获取所有节点的IP
                            const allIPs = await Promise.all(children.map(child => {
                                const fullPath = `${barrierPath}/${child}`;
                                return new Promise((resolve) => {
                                    client.getData(fullPath, (err, data) => {
                                        resolve(data ? data.toString() : '');
                                    });
                                });
                            }));
                            // 统计IP数量
                            const ipCount = _.countBy(allIPs);
                            const uniqueIPCount = Object.keys(ipCount).length;
                            console.log('当前不同IP数量:', uniqueIPCount);
                            // Top10 IP统计
                            const top10 = Object.entries(ipCount)
                                .sort((a, b) => b[1] - a[1])
                                .slice(0, 10);
                            console.log('Top10 IP:');
                            top10.forEach(([ip, count]) => {
                                console.log(`${ip}: ${count}`);
                            });
                            // 打印新增节点及其IP
                            for (const child of added) {
                                const idx = children.indexOf(child);
                                console.log('新增节点 id:', child, 'IP:', allIPs[idx]);
                            }
                        }
                        if (children.length >= participantCount) {
                            console.log('屏障已通过！所有参与者已就绪。');
                            resolve();
                        }
                        else {
                            console.log(`等待中，当前已就绪: ${children.length} / ${participantCount}`);
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
        await enterBarrier(client, barrierPath, participantCount);
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
