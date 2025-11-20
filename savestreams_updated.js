/* globals MessagePack, jsonpatch */
const { compare, applyPatch } = jsonpatch;

// pads the buffer to a specific length with null bytes
// params - buffer (Uint8Array): the input buffer to pad, multiple (int): the desired length multiple
// returns - paddedBuffer (Uint8Array): the padded buffer
function padTo(buffer, multiple) {
    if (multiple <= 0) {
        throw new Error("Padding multiple must be a positive integer");
    }

    // calculate required padding
    const padding = (multiple - (buffer.length % multiple)) % multiple;
    if (padding === 0) {
        return buffer;
    }

    // create new buffer with padding
    const paddedBuffer = new Uint8Array(buffer.length + padding);
    paddedBuffer.set(buffer, 0);
    // The remaining bytes are already initialized to 0 (null bytes)

    return paddedBuffer;
}

// split the v86 savestate into its header, info, and buffer blocks
// params - fileContent (Uint8Array): the complete v86 savestate buffer
// returns - Object{headerBlock: Uint8Array, infoBlock: Uint8Array, bufferBlock: Uint8Array} : the split savestate components
function splitV86Savestate(fileContent) {
    // v86 savestate header is the first 16 bytes
    const headerBlock = fileContent.subarray(0, 16);

    // read info length from header
    const headerView = new DataView(headerBlock.buffer, headerBlock.byteOffset, headerBlock.byteLength);
    const infoLength = headerView.getInt32(12, true); // little-endian, info length at 3*4 = byte 12

    // extract info block
    const infoBlock = fileContent.subarray(16, 16 + infoLength);

    // calculate buffer block offset, aligned to next multiple of 4
    let bufferOffset = 16 + infoLength;
    bufferOffset = (bufferOffset + 3) & ~3; // align to next multiple of 4

    // extract buffer block
    const bufferBlock = fileContent.subarray(bufferOffset);

    return {
        headerBlock,
        infoBlock,
        bufferBlock
    };
}

// recombine the v86 savestate components into a single buffer
// params - Object{headerBlock: Uint8Array, infoBlock: Uint8Array, bufferBlock: Uint8Array} : the savestate components
// returns - recombinedState (Uint8Array): the recombined v86 savestate buffer
function recombineV86Savestate(headerBlock, infoBlock, bufferBlock) {
    // calculate info block padding to align to 4 bytes
    const padding = ((infoBlock.length + 3) & ~3) - infoBlock.length;

    // create final buffer with total length
    const recombinedState = new Uint8Array(
        headerBlock.length + infoBlock.length + padding + bufferBlock.length
    );

    // populate final buffer
    recombinedState.set(headerBlock, 0);
    recombinedState.set(infoBlock, headerBlock.length);
    recombinedState.set(bufferBlock, headerBlock.length + infoBlock.length + padding);

    return recombinedState;
}

// align buffer blocks to a specific block size for efficient deduplication
// params - infoBlock (Uint8Array): the info block, bufferBlock (Uint8Array): the buffer block, blockSize (int): the desired alignment block size
// returns - alignedBufferBlock (Uint8Array): the aligned buffer block
function makeAlignedBufferBlock(infoBlock, bufferBlock, blockSize) {
    const info = JSON.parse(new TextDecoder("utf-8").decode(infoBlock));
    const alignedBlocks = [];

    for (const bufferInfo of info.buffer_infos) {
        const offset = bufferInfo.offset;
        const length = bufferInfo.length;

        const paddingLength = (blockSize - (length % blockSize)) % blockSize;
        const rawBlock = bufferBlock.subarray(offset, offset + length);

        // create new block with padding
        const paddedBlock = new Uint8Array(length + paddingLength);
        paddedBlock.set(rawBlock, 0);
        // remaining bytes are already null bytes

        alignedBlocks.push(paddedBlock);
    }

    // concatenate all aligned blocks into a single buffer
    const totalLength = alignedBlocks.reduce((sum, block) => sum + block.length, 0);
    const alignedBufferBlock = new Uint8Array(totalLength);

    let currentOffset = 0;
    for (const block of alignedBlocks) {
        alignedBufferBlock.set(block, currentOffset);
        currentOffset += block.length;
    }

    return alignedBufferBlock;
}

// convert an aligned buffer block back to its original unaligned form
// params - infoBlock (Uint8Array): the info block, alignedBufferBlock (Uint8Array): the aligned buffer block, blockSize (int): the alignment block size
// returns - unalignedBufferBlock (Uint8Array): the unaligned buffer block
function makeUnalignedBufferBlock(infoBlock, alignedBufferBlock, blockSize) {
    const info = JSON.parse(new TextDecoder("utf-8").decode(infoBlock));
    const unalignedBlocks = [];

    let offset = 0;
    for (const bufferInfo of info.buffer_infos) {
        const length = bufferInfo.length;
        const paddingLength = (blockSize - (length % blockSize)) % blockSize;
        const rawBlock = alignedBufferBlock.subarray(offset, offset + length); // extract without padding
        unalignedBlocks.push(rawBlock);
        offset += length + paddingLength; // move offset past padding
    }

    // NEW (FIXED) LOGIC: Reconstruct the original buffer block with 4-byte padding
    const totalLength = unalignedBlocks.reduce((sum, block) => sum + ((block.length + 3) & ~3), 0);
    const unalignedBufferBlock = new Uint8Array(totalLength);

    let currentOffset = 0;
    for (const block of unalignedBlocks) {
        unalignedBufferBlock.set(block, currentOffset);
        currentOffset += (block.length + 3) & ~3; // Add padding for the *next* block's offset
    }

    return unalignedBufferBlock;
}

/**
 * Creates a collision-free string key from a Uint8Array.
 * Using block.toString() (e.g., "1,2,3,4") is a reliable way to get a
 * unique key for a byte sequence in a JavaScript Map, analogous to
 * using raw bytes as a dict key in Python.
 * @param {Uint8Array} block The byte block
 * @returns {string} A unique string key
 */
function hashBlock(block) {
    return block.toString();
}

// encode a sequence of v86 savestates into a single compressed savestream
// params - savestatesIterator (Array or AsyncIterable of Uint8Array): the sequence of v86 savestate buffers
//          blockSize (int): the alignment block size
//          superBlockMultiple (int): the number of blocks per superblock
//          totalCount (int): optional total count for progress tracking (if iterator doesn't have length)
// returns - savestream (Uint8Array): the compressed savestream buffer
async function encode(savestatesIterator, {blockSize = 256, superBlockMultiple = 256, onProgress = null, totalCount = null} = {}) {

    // Use totalCount if provided, otherwise try .length (for Arrays), else 0
    const total = totalCount !== null ? totalCount : (savestatesIterator.length || 0);

    async function reportProgress(index, total) {
        if (onProgress) {
            onProgress(index, total);
        } 

        // Yield to event loop to keep UI responsive
        await new Promise(resolve => setTimeout(resolve, 0));
    }

    const superBlockSize = blockSize * superBlockMultiple;

    // make zero blocks
    const zeroBlock = new Uint8Array(blockSize);
    const zeroSuperBlock = new Uint8Array(superBlockSize);

    // maps known byte blocks and super blocks to their unique IDs
    const knownBlocks = new Map([[hashBlock(zeroBlock), 0]]);
    const knownSuperBlocks = new Map([[hashBlock(zeroSuperBlock), 0]]);

    // array to hold the encoded data
    const incrementalSaves = [];

    // keep tract of previous savestate info for diffing
    let prevInfo = {};

    await reportProgress(0, total);

    // main encoding loop (supports `for await` to handle both Arrays and Async Generators)
    for await (const savestate of savestatesIterator) {
        // split savestate into components
        const { headerBlock, infoBlock, bufferBlock } = splitV86Savestate(savestate);

        // align the buffer and break into fixed-size superblocks
        let alignedBufferBlock = makeAlignedBufferBlock(infoBlock, bufferBlock, blockSize);
        alignedBufferBlock = padTo(alignedBufferBlock, superBlockSize);

        // split aligned buffer into superblocks
        const superBlocks = [];
        for (let offset = 0; offset < alignedBufferBlock.length; offset += superBlockSize) {
            superBlocks.push(alignedBufferBlock.subarray(offset, offset + superBlockSize));
        }

        console.log(`Encoding savestate: ${superBlocks.length} superblocks of size ${superBlockSize} bytes`);

        const superIdSequence = [];
        const newSuperBlocks = new Map();
        const newBlocks = new Map();
        const encoder = new TextEncoder();

        // process each superblock
        for (const superBlock of superBlocks) {
            const superBlockKey = hashBlock(superBlock); // hash for key
            if (!knownSuperBlocks.has(superBlockKey)) {
                // new superblock found
                const superBlockId = knownSuperBlocks.size;
                knownSuperBlocks.set(superBlockKey, superBlockId);

                // further break down superblock into blocks
                const blockIds = [];
                for (let offset = 0; offset < superBlock.length; offset += blockSize) {
                    const block = superBlock.subarray(offset, offset + blockSize);
                    const blockKey = hashBlock(block); // hash for key
                    if (!knownBlocks.has(blockKey)) {
                        const blockId = knownBlocks.size;
                        knownBlocks.set(blockKey, blockId);
                        newBlocks.set(blockId, block.slice());
                    }
                    blockIds.push(knownBlocks.get(blockKey));
                }

                // store mapping of new superblock to its block IDs
                newSuperBlocks.set(superBlockId, blockIds);
            }

            // record superblock ID in sequence
            superIdSequence.push(knownSuperBlocks.get(superBlockKey));
        }

        // delta encode the info block
        const infoJson = JSON.parse(new TextDecoder("utf-8").decode(infoBlock));
        const infoDiff = compare(prevInfo, infoJson);
        const encodedInfo = encoder.encode(JSON.stringify(infoDiff));
        prevInfo = infoJson;

        // record the incremental save data
        incrementalSaves.push({
            headerBlock: headerBlock.slice(),
            infoPatch: encodedInfo,
            newBlocks: Object.fromEntries(newBlocks),
            newSuperBlocks: Object.fromEntries(newSuperBlocks),
            superIdSequence
        });

        await reportProgress(incrementalSaves.length, total);

    }

    // pack and return the entire savestream as MessagePack
    return MessagePack.encode(incrementalSaves);

}

// decode a savestream back into a sequence of v86 savestates
// params - savestream (Uint8Array): the compressed savestream buffer, blockSize (int): the alignment block size, superBlockMultiple (int): the number of blocks per superblock
// returns - savestatesArray (Array of Uint8Array): the array of v86 savestate buffers
async function decode(savestream, blockSize = 256, superBlockMultiple = 256) {
    const incrementalSaves = MessagePack.decode(savestream);

    const superBlockSize = blockSize * superBlockMultiple;

    // initialize known blocks and superblocks with zero blocks
    const zeroBlock = new Uint8Array(blockSize);

    const knownBlocks = new Map([[0, zeroBlock]]);
    const knownSuperBlocks = new Map([[0, Array(superBlockMultiple).fill(0)]]);

    let prevInfo = {};

    // iterate through each incremental save to reconstruct savestates
    function* generateSaves() {
        for (const save of incrementalSaves) {
            const { headerBlock, infoPatch, newBlocks, newSuperBlocks, superIdSequence } = save;

            for (const [blockId, blockData] of Object.entries(newBlocks)) {
                knownBlocks.set(Number(blockId), blockData);
            }

            for (const [superBlockId, blockIds] of Object.entries(newSuperBlocks)) {
                knownSuperBlocks.set(Number(superBlockId), blockIds);
            }

            // reconstruct the full info JSON using the patch and prevInfo
            const delta = JSON.parse(new TextDecoder("utf-8").decode(infoPatch));
            const currentInfo = applyPatch(prevInfo, delta).newDocument;
            prevInfo = currentInfo;

            const infoBlock = new TextEncoder().encode(JSON.stringify(currentInfo));

            // reconstruct the full aligned buffer from superblock sequence
            const alignedBufferBlock = new Uint8Array(superIdSequence.length * superBlockSize);
            let offset = 0;

            for (const superBlockId of superIdSequence) {
                const blockIds = knownSuperBlocks.get(superBlockId);

                for (const blockId of blockIds) {
                    const blockData = knownBlocks.get(blockId);
                    alignedBufferBlock.set(blockData, offset);
                    offset += blockSize;
                }

            }
            
            // convert aligned buffer back to unaligned buffer
            const unalignedBufferBlock = makeUnalignedBufferBlock(infoBlock, alignedBufferBlock, blockSize);

            // stitch together the final savestate
            const savestate = recombineV86Savestate(headerBlock, infoBlock, unalignedBufferBlock);

            yield savestate;

        }
    }

    return generateSaves();
}

// trim a savestream to include only a specific range of save states
// params:
//   savestream: Uint8Array - the compressed savestream
//   startIndex: number - starting index (inclusive)
//   endIndex?: number - ending index (exclusive); if undefined, goes to the end
// returns: Uint8Array - a new compressed savestream containing only the specified range
async function trim(savestream, startIndex, endIndex) {
    if (startIndex < 0) {
        throw new Error("Invalid start index");
    }

    const totalLen = await decodeLen(savestream);
    if (endIndex === undefined || endIndex === null) {
        endIndex = totalLen;
    } else if (endIndex < 0) {
        endIndex = totalLen + endIndex;
    }

    const trimmed = [];
    let i = 0;
    for await (const state of await decode(savestream)) {
        if (i >= startIndex && i < endIndex) {
            trimmed.push(state);
        }
        i++;
        if (i >= endIndex) break;
    }

    if (trimmed.length === 0) {
        throw new Error("No states in the specified range");
    }

    return encode(trimmed);
}

// decode a single save state from a savestream at the specified index
// params:
//   savestream: Uint8Array - the compressed savestream
//   index: number - the index of the save state to decode
// returns: Uint8Array - the decoded v86 savestate
async function decodeOne(savestream, index) {
    const totalLen = await decodeLen(savestream);
    if (index < 0 || index >= totalLen) {
        throw new RangeError(`Index ${index} out of range for savestream with ${totalLen} saves`);
    }

    let i = 0;
    for await (const state of await decode(savestream)) {
        if (i === index) return state;
        i++;
    }

    throw new RangeError(`Index ${index} out of range for savestream with ${totalLen} saves`);
}

// get the number of save states in a savestream
async function decodeLen(savestream) {
    const incrementalSaves = MessagePack.decode(savestream);
    return incrementalSaves.length;
}

window.v86Savestream = {
  encode,
  decode,
  decodeOne,
  trim,
  decodeLen,
  _internal: { // Export internal functions for testing
    padTo,
    splitV86Savestate,
    recombineV86Savestate,
    makeAlignedBufferBlock,
    makeUnalignedBufferBlock,
    hashBlock
  }
};