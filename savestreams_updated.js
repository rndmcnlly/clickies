/* globals MessagePack, jsonpatch */
const { compare, applyPatch } = jsonpatch;

// pads the buffer to a specific length with null bytes
function padTo(buffer, multiple) {
    if (multiple <= 0) {
        throw new Error("Padding multiple must be a positive integer");
    }
    const padding = (multiple - (buffer.length % multiple)) % multiple;
    if (padding === 0) return buffer;

    const paddedBuffer = new Uint8Array(buffer.length + padding);
    paddedBuffer.set(buffer, 0);
    return paddedBuffer;
}

// split the v86 savestate into its header, info, and buffer blocks
function splitV86Savestate(fileContent) {
    const headerBlock = fileContent.subarray(0, 16);
    const headerView = new DataView(headerBlock.buffer, headerBlock.byteOffset, headerBlock.byteLength);
    const infoLength = headerView.getInt32(12, true);
    const infoBlock = fileContent.subarray(16, 16 + infoLength);
    
    let bufferOffset = 16 + infoLength;
    bufferOffset = (bufferOffset + 3) & ~3;
    const bufferBlock = fileContent.subarray(bufferOffset);

    return { headerBlock, infoBlock, bufferBlock };
}

// recombine the v86 savestate components into a single buffer
function recombineV86Savestate(headerBlock, infoBlock, bufferBlock) {
    const padding = ((infoBlock.length + 3) & ~3) - infoBlock.length;
    const recombinedState = new Uint8Array(headerBlock.length + infoBlock.length + padding + bufferBlock.length);

    recombinedState.set(headerBlock, 0);
    recombinedState.set(infoBlock, headerBlock.length);
    recombinedState.set(bufferBlock, headerBlock.length + infoBlock.length + padding);

    return recombinedState;
}

// align buffer blocks to a specific block size for efficient deduplication
// OPTIMIZED: Calculates size first, allocates ONCE, and avoids intermediate arrays.
function makeAlignedBufferBlock(infoOrBytes, bufferBlock, blockSize, alignToTotalSize = 1) {
    const info = (infoOrBytes instanceof Uint8Array) 
        ? JSON.parse(new TextDecoder("utf-8").decode(infoOrBytes)) 
        : infoOrBytes;

    // 1. Calculate total content size
    let contentSize = 0;
    for (const bufferInfo of info.buffer_infos) {
        const length = bufferInfo.length;
        const paddingLength = (blockSize - (length % blockSize)) % blockSize;
        contentSize += length + paddingLength;
    }

    // 2. Calculate final size (including superBlock alignment if requested)
    let finalSize = contentSize;
    if (alignToTotalSize > 1) {
        const padding = (alignToTotalSize - (finalSize % alignToTotalSize)) % alignToTotalSize;
        finalSize += padding;
    }

    // 3. Allocate ONE buffer (Prevents OOM on large states)
    const alignedBufferBlock = new Uint8Array(finalSize);

    // 4. Copy data directly
    let currentOffset = 0;
    for (const bufferInfo of info.buffer_infos) {
        const length = bufferInfo.length;
        const offset = bufferInfo.offset;
        const paddingLength = (blockSize - (length % blockSize)) % blockSize;

        // Copy chunk directly from source to destination
        alignedBufferBlock.set(bufferBlock.subarray(offset, offset + length), currentOffset);
        
        currentOffset += length + paddingLength;
    }

    return alignedBufferBlock;
}

// convert an aligned buffer block back to its original unaligned form
// OPTIMIZED: Calculates size first and allocates ONCE.
function makeUnalignedBufferBlock(infoOrBytes, alignedBufferBlock, blockSize) {
    const info = (infoOrBytes instanceof Uint8Array) 
        ? JSON.parse(new TextDecoder("utf-8").decode(infoOrBytes)) 
        : infoOrBytes;

    // 1. Calculate total size (original format uses 4-byte alignment)
    let totalLength = 0;
    for (const bufferInfo of info.buffer_infos) {
        totalLength += (bufferInfo.length + 3) & ~3; 
    }

    const unalignedBufferBlock = new Uint8Array(totalLength);

    let alignedOffset = 0;
    let unalignedOffset = 0;

    for (const bufferInfo of info.buffer_infos) {
        const length = bufferInfo.length;
        const alignedPadding = (blockSize - (length % blockSize)) % blockSize;
        
        // Extract chunk
        const chunk = alignedBufferBlock.subarray(alignedOffset, alignedOffset + length);
        unalignedBufferBlock.set(chunk, unalignedOffset);

        // Advance pointers
        alignedOffset += length + alignedPadding;
        unalignedOffset += (length + 3) & ~3; 
    }

    return unalignedBufferBlock;
}

// Fast hash function that produces a fixed-size hex string
// Uses double FNV-1a hash (64-bit) for good distribution and very low collision probability
function hashBlock(block) {
    // FNV-1a hash constants
    const FNV_OFFSET_BASIS_1 = 2166136261;
    const FNV_OFFSET_BASIS_2 = 2166136261 ^ 0x5a5a5a5a; // Different seed for second hash
    const FNV_PRIME = 16777619;
    
    let hash1 = FNV_OFFSET_BASIS_1;
    let hash2 = FNV_OFFSET_BASIS_2;
    
    // Process block with two different hash functions
    for (let i = 0; i < block.length; i++) {
        const byte = block[i];
        hash1 ^= byte;
        hash1 = (hash1 * FNV_PRIME) >>> 0;
        hash2 ^= byte;
        hash2 = (hash2 * FNV_PRIME) >>> 0;
    }
    
    // Combine into 64-bit hash (16 hex characters)
    // This provides extremely low collision probability for 256-byte blocks
    return hash1.toString(16).padStart(8, '0') + hash2.toString(16).padStart(8, '0');
}

// encode a sequence of v86 savestates
async function encode(savestatesIterator, {blockSize = 256, superBlockMultiple = 256, onProgress = null, totalCount = null} = {}) {
    const total = totalCount !== null ? totalCount : (savestatesIterator.length || 0);

    async function reportProgress(index, total) {
        if (onProgress) onProgress(index, total);
        // Yield to event loop
        await new Promise(resolve => setTimeout(resolve, 0));
    }

    const superBlockSize = blockSize * superBlockMultiple;
    const zeroBlock = new Uint8Array(blockSize);
    const zeroSuperBlock = new Uint8Array(superBlockSize);

    const knownBlocks = new Map([[hashBlock(zeroBlock), 0]]);
    const knownSuperBlocks = new Map([[hashBlock(zeroSuperBlock), 0]]);
    const incrementalSaves = [];
    let prevInfo = {};

    // Yield immediately to allow UI to update before starting
    await reportProgress(0, total);

    for await (const savestate of savestatesIterator) {
        const { headerBlock, infoBlock, bufferBlock } = splitV86Savestate(savestate);
        
        // Optimization: Parse Info ONCE
        const infoJson = JSON.parse(new TextDecoder("utf-8").decode(infoBlock));

        // Optimization: Create aligned block AND pad to superBlockSize in one go
        const alignedBufferBlock = makeAlignedBufferBlock(infoJson, bufferBlock, blockSize, superBlockSize);

        // split aligned buffer into superblocks (views, cheap)
        const superBlocks = [];
        for (let offset = 0; offset < alignedBufferBlock.length; offset += superBlockSize) {
            superBlocks.push(alignedBufferBlock.subarray(offset, offset + superBlockSize));
        }

        const superIdSequence = [];
        const newSuperBlocks = new Map();
        const newBlocks = new Map();
        const encoder = new TextEncoder();

        // Process superblocks with periodic yielding to prevent main thread blocking
        const YIELD_EVERY_N_SUPERBLOCKS = 50; // Yield roughly every ~3MB processed (assuming 64KB superblocks)
        let loopsSinceYield = 0;

        for (const superBlock of superBlocks) {
            const superBlockKey = hashBlock(superBlock);
            
            if (!knownSuperBlocks.has(superBlockKey)) {
                const superBlockId = knownSuperBlocks.size;
                knownSuperBlocks.set(superBlockKey, superBlockId);

                const blockIds = [];
                for (let offset = 0; offset < superBlock.length; offset += blockSize) {
                    const block = superBlock.subarray(offset, offset + blockSize);
                    const blockKey = hashBlock(block);
                    if (!knownBlocks.has(blockKey)) {
                        const blockId = knownBlocks.size;
                        knownBlocks.set(blockKey, blockId);
                        newBlocks.set(blockId, block.slice());
                    }
                    blockIds.push(knownBlocks.get(blockKey));
                }
                newSuperBlocks.set(superBlockId, blockIds);
            }
            superIdSequence.push(knownSuperBlocks.get(superBlockKey));

            // ANTI-FREEZE: Yield to event loop
            loopsSinceYield++;
            if (loopsSinceYield >= YIELD_EVERY_N_SUPERBLOCKS) {
                loopsSinceYield = 0;
                await new Promise(resolve => setTimeout(resolve, 0));
            }
        }

        // delta encode the info block
        const infoDiff = compare(prevInfo, infoJson);
        const encodedInfo = encoder.encode(JSON.stringify(infoDiff));
        prevInfo = infoJson;

        incrementalSaves.push({
            headerBlock: headerBlock.slice(),
            infoPatch: encodedInfo,
            newBlocks: Object.fromEntries(newBlocks),
            newSuperBlocks: Object.fromEntries(newSuperBlocks),
            superIdSequence
        });

        await reportProgress(incrementalSaves.length, total);
    }

    return MessagePack.encode(incrementalSaves);
}

// decode a savestream
async function decode(savestream, blockSize = 256, superBlockMultiple = 256) {
    const incrementalSaves = MessagePack.decode(savestream);
    const superBlockSize = blockSize * superBlockMultiple;
    const zeroBlock = new Uint8Array(blockSize);

    const knownBlocks = new Map([[0, zeroBlock]]);
    const knownSuperBlocks = new Map([[0, Array(superBlockMultiple).fill(0)]]);

    let prevInfo = {};

    function* generateSaves() {
        for (const save of incrementalSaves) {
            const { headerBlock, infoPatch, newBlocks, newSuperBlocks, superIdSequence } = save;

            for (const [blockId, blockData] of Object.entries(newBlocks)) {
                knownBlocks.set(Number(blockId), blockData);
            }

            for (const [superBlockId, blockIds] of Object.entries(newSuperBlocks)) {
                knownSuperBlocks.set(Number(superBlockId), blockIds);
            }

            const delta = JSON.parse(new TextDecoder("utf-8").decode(infoPatch));
            const currentInfo = applyPatch(prevInfo, delta).newDocument;
            prevInfo = currentInfo;

            const infoBlock = new TextEncoder().encode(JSON.stringify(currentInfo));

            // Reconstruct full buffer
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
            
            // Convert back to V86 format (passing currentInfo object avoids re-parsing)
            const unalignedBufferBlock = makeUnalignedBufferBlock(currentInfo, alignedBufferBlock, blockSize);

            yield recombineV86Savestate(headerBlock, infoBlock, unalignedBufferBlock);
        }
    }

    return generateSaves();
}

// trim a savestream
async function trim(savestream, startIndex, endIndex) {
    if (startIndex < 0) throw new Error("Invalid start index");

    const totalLen = await decodeLen(savestream);
    if (endIndex === undefined || endIndex === null) endIndex = totalLen;
    else if (endIndex < 0) endIndex = totalLen + endIndex;

    const trimmed = [];
    let i = 0;
    for await (const state of await decode(savestream)) {
        if (i >= startIndex && i < endIndex) trimmed.push(state);
        i++;
        if (i >= endIndex) break;
    }

    if (trimmed.length === 0) throw new Error("No states in the specified range");
    return encode(trimmed);
}

// decode a single save state
async function decodeOne(savestream, index) {
    const totalLen = await decodeLen(savestream);
    if (index < 0 || index >= totalLen) throw new RangeError(`Index ${index} out of range`);

    let i = 0;
    for await (const state of await decode(savestream)) {
        if (i === index) return state;
        i++;
    }
    throw new RangeError(`Index ${index} out of range`);
}

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
  _internal: {
    padTo,
    splitV86Savestate,
    recombineV86Savestate,
    makeAlignedBufferBlock,
    makeUnalignedBufferBlock,
    hashBlock
  }
};