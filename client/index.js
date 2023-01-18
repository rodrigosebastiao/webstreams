const API_URL = "http://localhost:3000";

let counter = 0;


async function consumeAPI(signal) {
    const response = await fetch(API_URL, {
        signal
    });

    const reader = response.body
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(parseNDJSON())
        // .pipe(new WritableStream({
        //     write(chunk, controller) {
        //         console.log(++counter, "chunk", chunk);
        //     }
        // }));
        
    return reader;
}

function appendToHTML(element){
    return new WritableStream({
        write({title, description, url_anime}){
            const card = `
                <article>
                    <div class="text">
                        <h3> ${++counter}) ${title}</h3>
                        <p>${description}</p>
                        <a href="${url_anime}">Chck this out</a>
                    </div>
                </article>
            `;

            element.innerHTML += card;
        },
        abort(reason) {
            console.log("Aborted reason: ", reason);
        }
    })
}

function parseNDJSON() {
    let ndjsonBuffer = "";

    return new TransformStream({
        transform(chunk, controller) {
            ndjsonBuffer += chunk;

            const items = ndjsonBuffer.split("\n");
            items.slice(0, -1).forEach(item => controller.enqueue(JSON.parse(item)));

            ndjsonBuffer = items[items.length - 1];
        },
        flush(controller) {
            if(!ndjsonBuffer){ return; }
            controller.enqueue(JSON.parse(ndjsonBuffer));
        }
    });
}

const [
    start, 
    stop, 
    cards
] = ["start", "stop", "cards"].map(id => document.getElementById(id));
const abortController = new AbortController();

start.addEventListener("click", async () => {
    try {
        const readable = await consumeAPI(abortController.signal);
        // add signal and await to handle the abortError exception after abortion
        await readable.pipeTo(appendToHTML(cards), { signal: abortController.signal });
    } catch (error) {
        if (!error.message.includes('abort')) { throw error; }
    }
});


stop.addEventListener("click", () => {
    abortController.abort();
    console.log("...aborting");
    abortController = new AbortController();
});