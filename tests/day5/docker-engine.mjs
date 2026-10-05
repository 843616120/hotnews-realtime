import http from "node:http";

const socketPath = "\\\\.\\pipe\\docker_engine";
const services = new Set(["zookeeper", "kafka", "mysql", "redis", "jobmanager", "taskmanager"]);

function request(method, path, body) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? undefined : JSON.stringify(body);
    const req = http.request({
      socketPath, method, path,
      headers: payload === undefined ? {} : {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(payload),
      },
    }, res => {
      const chunks = [];
      res.on("data", chunk => chunks.push(chunk));
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        if (res.statusCode >= 400) reject(new Error(`${method} ${path}: ${res.statusCode} ${text}`));
        else resolve(text);
      });
    });
    req.on("error", reject);
    req.end(payload);
  });
}

async function containers() {
  const all = JSON.parse(await request("GET", "/containers/json?all=1"));
  const matching = all.filter(c => services.has(c.Labels["com.docker.compose.service"]));
  const groups = Map.groupBy(matching, c => c.Labels["com.docker.compose.project"]);
  const candidates = [...groups.values()].filter(group =>
    ["kafka", "mysql", "redis", "jobmanager", "taskmanager"].every(service =>
      group.some(c => c.Labels["com.docker.compose.service"] === service)));
  if (candidates.length !== 1) throw new Error(`Expected one complete hotnews Compose stack; found ${candidates.length}`);
  return candidates[0];
}

const [action, service, ...command] = process.argv.slice(2);
try {
  const group = await containers();
  if (action === "ps") {
    console.log(JSON.stringify(group.map(c => ({
      service: c.Labels["com.docker.compose.service"], name: c.Names[0], state: c.State, id: c.Id.slice(0, 12),
    })), null, 2));
  } else {
    if (!services.has(service)) throw new Error(`Unknown service: ${service}`);
    const container = group.find(c => c.Labels["com.docker.compose.service"] === service);
    if (!container) throw new Error(`Missing service: ${service}`);
    const base = `/containers/${container.Id}`;
    if (action === "exec" && command.length) {
      if (container.State !== "running") throw new Error(`${service} is not running`);
      if (service === "mysql" && !process.env.MYSQL_PASSWORD) {
        throw new Error("MySQL exec requires a user-supplied MYSQL_PASSWORD");
      }
      const { Id } = JSON.parse(await request("POST", `${base}/exec`, {
        AttachStdout: true, AttachStderr: true, Cmd: command, Tty: true,
        ...(service === "mysql" ? { Env: [`MYSQL_PWD=${process.env.MYSQL_PASSWORD}`] } : {}),
      }));
      const output = await request("POST", `/exec/${Id}/start`, { Detach: false, Tty: true });
      process.stdout.write(output);
      const result = JSON.parse(await request("GET", `/exec/${Id}/json`));
      if (result.ExitCode !== 0) process.exitCode = result.ExitCode || 1;
    } else if (action === "logs" && service === "taskmanager") {
      const tail = command[0] || "100";
      if (!/^\d{1,5}$/.test(tail)) throw new Error("logs tail must be a positive integer");
      process.stdout.write(await request("GET", `${base}/logs?stdout=1&stderr=1&tail=${tail}`));
    } else if (action === "kill" && service === "taskmanager") {
      await request("POST", `${base}/kill?signal=SIGKILL`);
      console.log(`Killed ${container.Names[0]}`);
    } else if (action === "start" && service === "taskmanager") {
      await request("POST", `${base}/start`);
      console.log(`Started ${container.Names[0]}`);
    } else {
      throw new Error("Usage: ps | exec <service> <command...> | logs taskmanager [tail] | kill taskmanager | start taskmanager");
    }
  }
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
