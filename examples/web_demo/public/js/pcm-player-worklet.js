class PCMPlayerProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = [];
    this.currentBuffer = null;
    this.inputPos = 0;
    this.inputRate = 24000;
    this.volume = 0.9;

    this.port.onmessage = (event) => {
      const msg = event.data;
      if (!msg || !msg.type) return;

      if (msg.type === 'push' && msg.data) {
        const buf = new Float32Array(msg.data);
        this.queue.push(buf);
        if (msg.sampleRate) {
          this.inputRate = msg.sampleRate;
        }
      } else if (msg.type === 'config') {
        if (typeof msg.volume === 'number') {
          this.volume = msg.volume;
        }
      }
    };
  }

  /**
   * 取出下一帧音频样本，支持跨 buffer。
   */
  nextSample(step) {
    while (this.currentBuffer === null || this.inputPos >= this.currentBuffer.length) {
      if (this.queue.length === 0) {
        return 0;
      }
      this.currentBuffer = this.queue.shift();
      this.inputPos = 0;
    }

    const idx = Math.floor(this.inputPos);
    const frac = this.inputPos - idx;
    const buf = this.currentBuffer;
    const s1 = buf[idx] || 0;
    const s2 = buf[Math.min(idx + 1, buf.length - 1)] || s1;
    const sample = s1 + (s2 - s1) * frac;
    this.inputPos += step;
    return sample * this.volume;
  }

  process(inputs, outputs) {
    const output = outputs[0];
    const channel = output[0];
    const step = this.inputRate / sampleRate; // 例如 24000/48000 = 0.5

    for (let i = 0; i < channel.length; i++) {
      channel[i] = this.nextSample(step);
    }

    if (this.queue.length === 0 && this.currentBuffer === null) {
      this.port.postMessage({ type: 'idle' });
    }

    return true;
  }
}

registerProcessor('pcm-player', PCMPlayerProcessor);
