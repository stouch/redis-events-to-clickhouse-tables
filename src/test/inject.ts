import Queue from "bull";
const queue = new Queue("event-track", "redis://127.0.0.1:6379");

queue.add(
  {
    event_type: "event_session",
    //toto: "zozo",
    zozo: "yolo",
    bibi: "bonjour",
    mais: 23,
    truc: new Date(),
    zaza: "2024-12-01",
    yolo: "2034 12 01",
    momo: "02/02/2045",
  },
  {
    attempts: 3,
  }
);

queue.add({
  event_type: "event_session",
  objectMoche: {
    profond: 1,
    dest: true,
  },
  manger: ["dupain"],
  "ba cest un": "test",
  toto: true,
  zozo: "yolo",
  bibi: false,
  mais: 23,
  truc: new Date(),
  zaza: "2024-12-01",
  yolo: "2034 12 01",
  momo: "02/02/2045",
});

/*
const run = async () => {
  while (true) {
    queue.add({
      event_type: "event_session_hello",
      toto: true,
      zozo: "yolo",
      bibi: false,
      wesh: "bon",
      mais: 23,
      truc: new Date(),
    });

    await new Promise((resolve) => {
      setTimeout(() => {
        resolve(true);
      }, 5);
    });
  }
};

run();

*/
