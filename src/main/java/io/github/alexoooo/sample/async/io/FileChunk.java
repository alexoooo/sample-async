package io.github.alexoooo.sample.async.io;

//-----------------------------------------------------------------------------------------------------------------
public class FileChunk {
    public final byte[] bytes;
    public int length;

    public FileChunk(int chunkSize) {
        bytes = new byte[chunkSize];
        length = 0;
    }
}
