/* globals V86, MediaRecorder, MessagePack, jsonpatch, v86Savestream */

/**
 * AlbumCapturer class manages the recording of v86 emulator sessions,
 * including video, input stimuli, and savestates, into an "album" folder.
 */
class AlbumCapturer {
    /**
     * Creates an instance of AlbumCapturer.
     * @param {V86} emulator - The v86 emulator instance.
     * @param {object} v86_specs - The machine specs used to initialize v86 (for the manifest).
     */
    constructor(emulator, v86_specs) {
        this.emulator = emulator;
        this.v86_specs = v86_specs;

        const MB = 1024 * 1024;
        
        switch (v86_specs.memory_size) {
            case 32 * MB:  // DSL
                this.minFreeSpace = 170 * MB;
                break;
            case 64 * MB:  // MS-DOS
                this.minFreeSpace = 200 * MB;
                break;
            case 256 * MB: // Android
                this.minFreeSpace = 630 * MB;
                break;
            default:
                this.minFreeSpace = 200 * MB; // Safe default
                break;
        }

        this.canvas = null;
        this.keycodes = {};

        // Recording state
        this.mediaRecorder = null;
        this.recordedChunks = [];
        this.stimulus = [];
        this.startTime = undefined;
        this.stateSequenceFilenames = [];
        this.saveStateInterval = null;

        // Filesystem handles
        this.albumDirHandle = null;
        this.privateDirHandle = null;
        this.currentFolder = null;

        // Status
        this.isRecording = false;
        this.guid = null;
        this.startedRecording = null;
        this.onQuotaExceeded = null;
    }

    /**
     * Initializes the capturer by fetching scancodes and registering emulator listeners.
     * @param {HTMLCanvasElement} canvas - The canvas element used by the emulator for video capture.
     */
    async init(canvas) {
        if (!canvas) {
            throw new Error("Canvas element is required for init.");
        }
        this.canvas = canvas;

        try {
            const res = await fetch("ps2-scancodes.json");
            this.keycodes = await res.json();
        } catch (e) {
            console.error("Failed to load ps2-scancodes.json", e);
        }
        
        this._registerEmulatorListeners();
    }

    /**
     * Registers listeners for keyboard and mouse events from the emulator.
     * @private
     */
    _registerEmulatorListeners() {
        // Check if the V86 structure is ready (it should be if called from emulator-loaded)
        if (!this.emulator.v86 || !this.emulator.v86.cpu || !this.emulator.v86.cpu.devices.ps2) {
            console.warn("Emulator not fully loaded yet, input capture may fail. Make sure to call init() after 'emulator-loaded'.");
            return;
        }

        // Attach directly to the PS2 bus
        this.emulator.v86.cpu.devices.ps2.bus.register(
            "keyboard-code",
            (code) => {
                let hexcode = "0x" + code.toString(16).toUpperCase();
                this._appendStimulusEvent({
                    name: "keyboard-code",
                    code: hexcode,
                    description: this.keycodes[hexcode],
                });
            }
        );

        this.emulator.v86.cpu.devices.ps2.bus.register("mouse-click", (data) => {
            this._appendStimulusEvent({
                name: "mouse-click",
                coordinates: [data[0], data[1]],
            });
        });

        this.emulator.v86.cpu.devices.ps2.bus.register("mouse-delta", (data) => {
            this._appendStimulusEvent({
                name: "mouse-delta",
                coordinates: [data[0], data[1]],
            });
        });
        
        console.log("Input listeners registered.");
    }

    /**
     * Prompts the user to select an album directory and creates a manifest.json if it doesn't exist.
     * @returns {Promise<boolean>} - True if the album was successfully created or opened.
     */
    async createAlbum() {
        try {
            this.albumDirHandle = await window.showDirectoryPicker();

            try {
                await this.albumDirHandle.getFileHandle("manifest.json");
                console.log("Using existing manifest");
            } catch (error) {
                const manifestHandle = await this.albumDirHandle.getFileHandle("manifest.json", { create: true });
                const writable = await manifestHandle.createWritable();
                const initialManifest = {
                    "machine_spec": this.v86_specs,
                    "clips": []
                };
                await writable.write(JSON.stringify(initialManifest, null, 2));
                await writable.close();
                console.log("Created new manifest");
            }
            return true;
        } catch (e) {
            console.error("Failed to create album:", e);
            return false;
        }
    }

    /**
     * Starts recording a new clip. Creates a new subfolder and begins capturing
     * video, inputs, and periodic savestates.
     */
    async startRecording() {
        if (!this.albumDirHandle) {
            alert("Please create or open an album folder first.");
            return;
        }
        
        this.guid = crypto.randomUUID();
        this.currentFolder = await this.albumDirHandle.getDirectoryHandle(this.guid, { create: true });
        this.startedRecording = new Date().toISOString();
        this.isRecording = true;

        this._startInputRecording();
        this._startVideoRecording();
        await this._startStateRecording();
        
        console.log(`Recording started. Clip ID: ${this.guid}`);
    }

// 1. Update stopRecording to accept the callback
    async stopRecording(onProgress) {
        if (!this.isRecording) return;
        
        console.log("Stopping recording...");

        let clickieName = prompt("Clickie Name:") || "";

        try {
            // Finalize recordings
            const stimulusContent = this._finishInputRecording();
            const responseBlob = await this._stopVideoRecording();
            
            // 2. Pass the callback down
            const savestreamBuffer = await this._stopStateRecordingAndEncode(onProgress);

            // Write stimulus.vtt
            const stimulusHandle = await this.currentFolder.getFileHandle("stimulus.vtt", { create: true });
            const writableStimulus = await stimulusHandle.createWritable();
            await writableStimulus.write(stimulusContent);
            await writableStimulus.close();

            // Write response.webm
            const responseHandle = await this.currentFolder.getFileHandle("response.webm", { create: true });
            const writableResponse = await responseHandle.createWritable();
            await writableResponse.write(await responseBlob.arrayBuffer());
            await writableResponse.close();

            // Write states.savestream
            const saveStreamHandle = await this.currentFolder.getFileHandle("states.savestream", { create: true });
            const writableSaveStream = await saveStreamHandle.createWritable();
            await writableSaveStream.write(savestreamBuffer);
            await writableSaveStream.close();

            // Update manifest.json
            const manifestHandle = await this.albumDirHandle.getFileHandle("manifest.json");
            const manifestFile = await manifestHandle.getFile();
            const manifest = JSON.parse(await manifestFile.text());

            manifest.clips.push({
                id: this.guid,
                timestamp: this.startedRecording,
                name: clickie_name,
            });

            const writableManifest = await manifestHandle.createWritable();
            await writableManifest.write(JSON.stringify(manifest, null, 2));
            await writableManifest.close();
            
            console.log("Clip saved successfully.");
            return true;
        } catch (e) {
            console.error("Failed to stop recording:", e);
            alert("Error saving recording: " + e.message); // Alert the user
            return false; // <--- Return FALSE on failure
        } finally {
            // Reset state
            this.isRecording = false;
            this.guid = null;
            this.startedRecording = null;
            this.currentFolder = null;
        }
    }

    // --- Private Methods ---

    /**
     * Appends a stimulus event to the recording buffer.
     * @param {object} event - The input event to record.
     * @private
     */
    _appendStimulusEvent(event) {
        if (this.isRecording) {
            let timestamp = Date.now();
            let item = { event, timestamp };
            console.log("Appending item:", item);
            this.stimulus.push(item);
        }
    }

    /**
     * Initializes the input recording state.
     * @private
     */
    _startInputRecording() {
        this.stimulus = [];
        this.startTime = Date.now();
    }

    /**
     * Finalizes input recording and returns a WebVTT-formatted string.
     * @returns {string} - The stimulus track as a VTT string.
     * @private
     */
    _finishInputRecording() {
        let fileLines = ["WEBVTT", ""];
        for (let event of this.stimulus) {
            let timestamp = event.timestamp;
            let timeElapsed = timestamp - this.startTime;
            fileLines.push(`${this._formatWebVttTimestamp(timeElapsed)} --> ${this._formatWebVttTimestamp(timeElapsed + 1)}`);
            fileLines.push(JSON.stringify(event));
            fileLines.push("");
        }
        return fileLines.join("\n");
    }

    /**
     * Formats milliseconds into a WebVTT timestamp string (HH:MM:SS.mmm).
     * @param {number} ms - The timestamp in milliseconds.
     * @returns {string} - The formatted timestamp.
     * @private
     */
    _formatWebVttTimestamp(ms) {
        const milliseconds = ms % 1000;
        const totalSeconds = Math.floor(ms / 1000);
        const seconds = totalSeconds % 60;
        const totalMinutes = Math.floor(totalSeconds / 60);
        const minutes = totalMinutes % 60;
        const hours = Math.floor(totalMinutes / 60);
        const pad = (n, z = 2) => String(n).padStart(z, "0");
        return `${pad(hours)}:${pad(minutes)}:${pad(seconds)}.${pad(milliseconds, 3)}`;
    }

    /**
     * Starts video recording using MediaRecorder.
     * @private
     */
    _startVideoRecording() {
        this.recordedChunks = [];
        const stream = this.canvas.captureStream(30); // 30 FPS
        this.mediaRecorder = new MediaRecorder(stream, {
            mimeType: 'video/webm;codecs=vp9',
            videoBitsPerSecond: 2500000
        });
        this.mediaRecorder.ondataavailable = event => {
            if (event.data.size > 0) this.recordedChunks.push(event.data);
        };
        this.mediaRecorder.start(100);
        console.log('Video recording in progress...');
    }

    /**
     * Stops video recording and returns a Blob of the recorded video.
     * @returns {Promise<Blob>} - A promise that resolves with the video Blob.
     * @private
     */
    _stopVideoRecording() {
        return new Promise((resolve, reject) => {
            if (!this.mediaRecorder || !this.isRecording) {
                reject(new Error('No video recording in progress.'));
                return;
            }
            this.mediaRecorder.onstop = () => {
                const blob = new Blob(this.recordedChunks, { type: 'video/webm' });
                console.log('Video recording stopped, blob ready.');
                resolve(blob);
            };
            this.mediaRecorder.onerror = (e) => reject(e.error || new Error('MediaRecorder error'));
            this.mediaRecorder.stop();
        });
    }

    /**
     * Saves a single savestate to the private file system.
     * @private
     */
    async _saveState() {
        if (navigator.storage && navigator.storage.estimate) {
            const estimate = await navigator.storage.estimate();
            const remaining = estimate.quota - estimate.usage;
            
            // Stop if less than 200MB remaining
            if (remaining < this.minFreeSpace) {
                console.warn(`Low storage (${(remaining/1024/1024).toFixed(0)}MB). Triggering quota exceeded handler.`);
                
                if (this.onQuotaExceeded) {
                    // Trigger the callback
                    this.onQuotaExceeded(); 
                }
                
                clearInterval(this.saveStateInterval); // Stop saving immediately
                return;
            }
        }

        const i = this.stateSequenceFilenames.length;
        const filename = `v86state (${i}).bin`;
        try {
            // 3. Perform the slow operations
            const data = await this.emulator.save_state();
            const fileHandle = await this.privateDirHandle.getFileHandle(filename, { create: true });
            const writable = await fileHandle.createWritable();
            await writable.write(data);
            await writable.close();

            // 4. SUCCESS! Now (and only now) do we add it to the official list.
            // This guarantees the encoder never sees a broken file.
            this.stateSequenceFilenames.push(filename);
            
            // Optional: Log the event only on success
            this._appendStimulusEvent({ name: "save-state", index: i });

        } catch (e) {
            console.warn(`Failed to save state ${i}:`, e);
            // If it fails, we do NOTHING. The state is skipped.
            // The encoder will simply encode states 1..10, skip 11, and continue with 12..26.
            
            // Attempt cleanup of the partial file
            try { await this.privateDirHandle.removeEntry(filename); } catch (_) {}
        }
    }

    /**
     * Starts the periodic saving of savestates.
     * @private
     */
    async _startStateRecording() {
        this.privateDirHandle = await navigator.storage.getDirectory();
        this.stateSequenceFilenames = [];
        // this.nextStateIndex = 0;

        await this._saveState(); // Save initial state
        this.saveStateInterval = setInterval(() => this._saveState(), 1000);
    }

    /**
     * Stops saving states, reads all states from the private FS,
     * encodes them into a single savestream, and cleans up the private FS.
     * @param {function} onProgress - Callback for progress updates during encoding.
     * @returns {Promise<Uint8Array>} - The encoded savestream.
     * @private
     */
    async _stopStateRecordingAndEncode(onProgress) {
        clearInterval(this.saveStateInterval);
        
        const sortedFilenames = [...this.stateSequenceFilenames].sort((a, b) => {
             const numA = parseInt(a.match(/\((\d+)\)/)[1], 10);
             const numB = parseInt(b.match(/\((\d+)\)/)[1], 10);
             return numA - numB;
        });

        // GENERATOR: Stream states one by one instead of loading all into an array
        const self = this;
        async function* stateGenerator() {
            console.log("Reading states from private FS...");
            for (let filename of sortedFilenames) {
                const handle = await self.privateDirHandle.getFileHandle(filename);
                const file = await handle.getFile();
                const buffer = new Uint8Array(await file.arrayBuffer());
                yield buffer;
                // Delete file immediately after yielding to free disk/memory pressure
                await self.privateDirHandle.removeEntry(filename);
            }
        }

        console.log(`Encoding ${sortedFilenames.length} states...`);
        if (!window.v86Savestream || !window.v86Savestream.encode) {
            throw new Error("v86Savestream.encode is not available. Did savestreams_updated.js load?");
        }

        // Pause Emulator
        const wasRunning = this.emulator.is_running();
        if (wasRunning) {
            await this.emulator.stop();
        }

        // Encode Savestream using the Generator
        const encodedStream = await window.v86Savestream.encode(stateGenerator(), { 
            onProgress: onProgress,
            totalCount: sortedFilenames.length // Pass total explicitly since generator lacks .length
        });
        
        // Resume Emulator
        if (wasRunning) {
            await this.emulator.run();
        }
        
        return encodedStream;
    }
}
