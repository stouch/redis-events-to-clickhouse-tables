import Queue from "bull";
import cluster from "cluster";

const numWorkers = 2;

const queue = new Queue("event-track", "redis://127.0.0.1:6379");

if (cluster.isMaster) {
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork();
  }

  cluster.on("online", function (_worker) {
    console.log("is online");
  });

  cluster.on("exit", function (worker, _code, _signal) {
    console.log("worker " + worker.process.pid + " died");
  });
} else {
  queue.process(function (job, jobDone) {
    console.log("Job done by worker", cluster.worker.id, job.id);
    console.log("job", job);
    jobDone();
  });
}
