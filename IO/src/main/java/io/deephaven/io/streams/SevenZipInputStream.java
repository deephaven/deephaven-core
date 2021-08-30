/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import SevenZip.CRC;
import SevenZip.Compression.LZ.OutWindow;
import SevenZip.Compression.LZMA.Base;
import SevenZip.Compression.RangeCoder.BitTreeDecoder;
import io.deephaven.io.InputStreamFactory;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.base.Reference;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipException;

// --------------------------------------------------------------------
/**
 * {@link InputStream} that can read 7zip archives (.7z) (partial implementation).
 */
public class SevenZipInputStream extends InputStream {

    public static final int UBYTE_TO_INT = 0xFF;
    public static final long UBYTE_TO_LONG = 0xFFL;
    public static final int USHORT_TO_INT = 0xFFFF;
    public static final long UINT_TO_LONG = 0xFFFFFFFFL;

    // 7zHeader.cpp
    private static final byte[] SIGNATURE =
        {0x37, 0x7A, (byte) 0xBC, (byte) 0xAF, (byte) 0x27, (byte) 0x1C};
    public static final long SIGNATURE_AS_LONG =
        (SIGNATURE[0] & UBYTE_TO_LONG) | ((SIGNATURE[1] & UBYTE_TO_LONG) << 8)
            | ((SIGNATURE[2] & UBYTE_TO_LONG) << 16) | ((SIGNATURE[3] & UBYTE_TO_LONG) << 24)
            | ((SIGNATURE[4] & UBYTE_TO_LONG) << 32) | ((SIGNATURE[5] & UBYTE_TO_LONG) << 40);
    private static final int SIGNATURE_LENGTH = SIGNATURE.length; // 7zHeader.h

    private static final byte ARCHIVE_VER_MAJOR = 0; // 7zHeader.h

    private static final int NUM_MAX = 0x7FFFFFFF; // 7zItem.h
    private static final int NUM_NO_INDEX = 0xFFFFFFFF; // 7zItem.h

    public static final int START_HEADER_LENGTH = 8 + 8 + 4; // 7zHeader.h
    public static final int START_HEADER_CRC_LENGTH = 4;
    private static final int VERSION_INFO_LENGTH = 2;

    // ################################################################
    // Archive header / "database" parsing

    // 7zHeader.h (NID::EEnum)
    private interface BlockType {
        int END = 0x00;
        int HEADER = 0x01;
        int ARCHIVE_PROPERTIES = 0x02;
        int ADDITIONAL_STREAMS_INFO = 0x03;
        int MAIN_STREAMS_INFO = 0x04;
        int FILES_INFO = 0x05;
        int PACK_INFO = 0x06;
        int UNPACK_INFO = 0x07;
        int SUBSTREAMS_INFO = 0x08;
        int SIZE = 0x09;
        int CRC = 0x0A;
        int FOLDER = 0x0B;
        int CODERS_UNPACK_SIZE = 0x0C;
        int NUM_UNPACK_STREAM = 0x0D;
        int EMPTY_STREAM = 0x0E;
        int EMPTY_FILE = 0x0F;
        int ANTI = 0x10;
        int NAME = 0x11;
        int CREATION_TIME = 0x12;
        int LAST_ACCESS_TIME = 0x13;
        int LAST_WRITE_TIME = 0x14;
        int WIN_ATTRIBUTES = 0x15;
        int COMMENT = 0x16;
        int ENCODED_HEADER = 0x17;
        int START_POS = 0x18;
    }

    /**
     * An archive consists of:
     * <OL>
     * <LI>A list of packed streams.
     * <LI>A list of folders, each of which consumes one or more of the packed streams (in order)
     * and produces one output unpacked stream.
     * <LI>A list of substream counts and lengths. Each unpacked stream (in order) is split into one
     * or more substreams.
     * <LI>A list of files. Each file (in order) may or may not consume one substream. (Directories
     * and anit-files do not consume a stream.) Files have things like names, timestamps,
     * attributes, etc.
     * </OL>
     */
    // 7zItem.h
    private static class ArchiveDatabase {
        /** Lengths of each packed stream. */
        List<Long> PackSizes = new LinkedList<Long>();
        /** CRCs of each packed stream. */
        List<Integer> PackCRCs = new LinkedList<Integer>();
        /** List of folders in archive. */
        List<Folder> Folders = new LinkedList<Folder>();
        /** Number of substreams in each unpacked stream (folder). */
        List<Integer> NumUnpackStreamsVector = new LinkedList<Integer>();
        /** List of files in archive. */
        List<FileItem> Files = new LinkedList<FileItem>();

        public void clear() {
            PackSizes.clear();
            PackCRCs.clear();
            Folders.clear();
            NumUnpackStreamsVector.clear();
            Files.clear();
        }
    }

    // 7zIn.h
    private static class ArchiveDatabaseEx extends ArchiveDatabase {
        InArchiveInfo ArchiveInfo = new InArchiveInfo();
        /** Offsets to the beginning of each packed stream. */
        List<Long> PackStreamStartPositions = new LinkedList<Long>();
        /** Index of the first packed stream for this folder. */
        List<Integer> FolderStartPackStreamIndex = new LinkedList<Integer>();
        /** Index of the first file for this folder. */
        List<Integer> FolderStartFileIndex = new LinkedList<Integer>();
        /** Index of the folder containing this file. */
        List<Integer> FileIndexToFolderIndexMap = new LinkedList<Integer>();

        @Override
        public void clear() {
            super.clear();
            ArchiveInfo.clear();
            PackStreamStartPositions.clear();
            FolderStartPackStreamIndex.clear();
            FolderStartFileIndex.clear();
            FileIndexToFolderIndexMap.clear();
        }

        public void fill() throws ZipException {
            fillFolderStartPackStream();
            fillStartPos();
            fillFolderStartFileIndex();
        }

        private void fillFolderStartPackStream() {
            FolderStartPackStreamIndex.clear();
            int startPos = 0;
            for (Folder Folder : Folders) {
                FolderStartPackStreamIndex.add(startPos);
                startPos += Folder.PackStreams.size();
            }
        }

        private void fillStartPos() {
            PackStreamStartPositions.clear();
            long startPos = 0;
            for (Long PackSize : PackSizes) {
                PackStreamStartPositions.add(startPos);
                startPos += PackSize;
            }
        }

        private void fillFolderStartFileIndex() throws ZipException {
            FolderStartFileIndex.clear();
            FileIndexToFolderIndexMap.clear();

            int folderIndex = 0;
            int indexInFolder = 0;
            for (int i = 0; i < Files.size(); i++) {
                FileItem file = Files.get(i);
                boolean emptyStream = !file.HasStream;
                if (emptyStream && indexInFolder == 0) {
                    FileIndexToFolderIndexMap.add(NUM_NO_INDEX);
                    continue;
                }
                if (indexInFolder == 0) {
                    // v3.13 incorrectly worked with empty folders
                    // v4.07: Loop for skipping empty folders
                    while (true) {
                        if (folderIndex >= Folders.size()) {
                            throw new ZipException("Bad header.");
                        }
                        FolderStartFileIndex.add(i); // check it
                        if (NumUnpackStreamsVector.get(folderIndex) != 0) {
                            break;
                        }
                        folderIndex++;
                    }
                }
                FileIndexToFolderIndexMap.add(folderIndex);
                if (emptyStream) {
                    continue;
                }
                indexInFolder++;
                if (indexInFolder >= NumUnpackStreamsVector.get(folderIndex)) {
                    folderIndex++;
                    indexInFolder = 0;
                }
            }
        }

        public long getFolderStreamPos(int folderIndex, int indexInFolder) {
            return ArchiveInfo.DataStartPosition + PackStreamStartPositions
                .get(FolderStartPackStreamIndex.get(folderIndex) + indexInFolder);
        }

        public long getFolderFullPackSize(int folderIndex) {
            int packStreamIndex = FolderStartPackStreamIndex.get(folderIndex);
            Folder folder = Folders.get(folderIndex);
            long size = 0;
            for (int i = 0; i < folder.PackStreams.size(); i++) {
                size += PackSizes.get(packStreamIndex + i);
            }
            return size;
        }

        public long getFolderPackStreamSize(int folderIndex, int streamIndex) {
            return PackSizes.get(FolderStartPackStreamIndex.get(folderIndex) + streamIndex);
        }

        public long getFilePackSize(int fileIndex) {
            int folderIndex = FileIndexToFolderIndexMap.get(fileIndex);
            if (folderIndex >= 0) {
                if (FolderStartFileIndex.get(folderIndex) == fileIndex) {
                    return getFolderFullPackSize(folderIndex);
                }
            }
            return 0;
        }
    }

    // 7zHeader.h
    private static class ArchiveVersion {
        byte Major;
        byte Minor;
    }

    // 7zIn.h
    private static class InArchiveInfo {
        ArchiveVersion Version = new ArchiveVersion();
        long StartPosition;
        long StartPositionAfterHeader;
        long DataStartPosition;
        long DataStartPosition2;
        List<Long> FileInfoPopIDs = new LinkedList<Long>();

        public void clear() {
            FileInfoPopIDs.clear();
        }
    }

    /**
     * A Folder is one compressed chunk of data. A folder has one codec and the cyphertext is a
     * small number of packed streams (usually one). Since the plaintext is one stream, a folder has
     * one CRC. The folder's plaintext stream will often be the concatenation of a bunch of files,
     * but the Folder knows nothing of this.
     * <P>
     * A <U>codec</U> (my term) is a small graph of coders. A <U>coder</U> does a transform from n
     * input streams to m output streams. <U>Bind pairs</U> attach the output stream of one coder to
     * the input stream of another coder. A codec always has one (unbound) output stream, but can
     * have many (unboud) input streams. The most common codec consists of one coder with one input
     * stream, one output stream, and no bind pairs.
     * 
     * <P>
     * Input streams and output streams are numbered in the order the coders are listed. PackStreams
     * is used to map from the (implied) list of packed (input) streams in the archive to the input
     * streams of the coders.
     */
    // 7zItem.h
    private static class Folder {

        /**
         * List of coders. Input and output stream indices (as referenced by the BindPairs) are
         * defined by the order of this list.
         */
        List<CoderInfo> Coders = new LinkedList<CoderInfo>();

        /**
         * List of BindPairs. Bind pairs attach the output stream of one coder to the input stream
         * of another coder. Stream indices are defined by the coders list.
         */
        List<BindPair> BindPairs = new LinkedList<BindPair>();

        /**
         * Map [ packed stream index (in this folder) -&gt; input stream index (in the list of
         * coders) ]
         */
        List<Integer> PackStreams = new LinkedList<Integer>();

        /**
         * Lengths of the output streams from the coders. Includes bound and unbound output streams.
         */
        List<Long> UnpackSizes = new LinkedList<Long>();

        /** CRC for the entire codec output stream. */
        Integer UnpackCRC;

        public long getUnpackSize() throws ZipException {
            if (UnpackSizes.isEmpty()) {
                return 0;
            }
            for (int i = UnpackSizes.size() - 1; i >= 0; i--) {
                if (findBindPairForOutStream(i) < 0) {
                    return UnpackSizes.get(i);
                }
            }
            throw new ZipException("Could not determine unpacked size for folder.");
        }

        public int getNumOutStreams() {
            int nResult = 0;
            for (CoderInfo Coder : Coders) {
                nResult += Coder.NumOutStreams;
            }
            return nResult;
        }

        public int findBindPairForInStream(long inStreamIndex) {
            for (int i = 0; i < BindPairs.size(); i++) {
                if (BindPairs.get(i).InIndex == inStreamIndex) {
                    return i;
                }
            }
            return -1;
        }

        private int findBindPairForOutStream(int outStreamIndex) {
            for (int i = 0; i < BindPairs.size(); i++) {
                if (BindPairs.get(i).OutIndex == outStreamIndex) {
                    return i;
                }
            }
            return -1;
        }

        public int findPackStreamArrayIndex(int inStreamIndex) {
            for (int i = 0; i < PackStreams.size(); i++) {
                if (PackStreams.get(i) == inStreamIndex) {
                    return i;
                }
            }
            return -1;
        }
    }

    // 7zItem.h
    private static class FileItem {
        long CreationTime; // note, this is an NT file time
        long LastWriteTime; // note, this is an NT file time
        long LastAccessTime; // note, this is an NT file time
        long UnPackSize;
        long StartPos;
        int Attributes;
        Integer FileCRC;
        String Name;

        boolean HasStream; // Test it !!! it means that there is
        // stream in some folder. It can be empty stream
        boolean IsDirectory;
        boolean IsAnti;
        boolean AreAttributesDefined;
        boolean IsCreationTimeDefined;
        boolean IsLastWriteTimeDefined;
        boolean IsLastAccessTimeDefined;
        boolean IsStartPosDefined;

        void setAttributes(int attributes) {
            AreAttributesDefined = true;
            Attributes = attributes;
        }

        void setCreationTime(long creationTime) {
            IsCreationTimeDefined = true;
            CreationTime = creationTime;
        }

        void setLastWriteTime(long lastWriteTime) {
            IsLastWriteTimeDefined = true;
            LastWriteTime = lastWriteTime;
        }

        void setLastAccessTime(long lastAccessTime) {
            IsLastAccessTimeDefined = true;
            LastAccessTime = lastAccessTime;
        }
    }

    // 7zItem.h
    private static class CoderInfo {
        int NumInStreams;
        int NumOutStreams;
        List<AltCoderInfo> AltCoders = new LinkedList<AltCoderInfo>();

        public boolean isSimpleCoder() {
            return (NumInStreams == 1) && (NumOutStreams == 1);
        }
    }

    // 7zItem.h
    private static class AltCoderInfo {
        MethodID MethodID = new MethodID();
        byte[] Properties;
    }

    // 7zMethodID.h
    private static class MethodID {
        byte[] ID;
        byte IDSize;

        public MethodID() {}

        public MethodID(byte... id) {
            ID = id;
            IDSize = (byte) id.length;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that == null || getClass() != that.getClass()) {
                return false;
            }
            MethodID methodID = (MethodID) that;
            if (IDSize != methodID.IDSize) {
                return false;
            }
            return Arrays.equals(ID, methodID.ID);
        }

        @Override
        public int hashCode() {
            int result = (int) IDSize;
            result = 29 * result + Arrays.hashCode(ID);
            return result;
        }
    }

    // 7zItem.h
    private static class BindPair {
        int InIndex;
        int OutIndex;
    }

    // 7zIn.h
    private static class InArchive {

        // 7zIn.cpp
        public void readDatabase(InputStream inputStream, ArchiveDatabaseEx database,
            InputStreamFactory inputStreamFactory, int nBeginStreamPosition) throws IOException {
            database.clear();
            database.ArchiveInfo.StartPosition = nBeginStreamPosition;

            ByteBuffer byteBuffer = readToByteBuffer(inputStream, VERSION_INFO_LENGTH);
            database.ArchiveInfo.Version.Major = byteBuffer.get();
            database.ArchiveInfo.Version.Minor = byteBuffer.get();
            if (ARCHIVE_VER_MAJOR != database.ArchiveInfo.Version.Major) {
                throw new ZipException("Archive version mismatch.");
            }

            byteBuffer = readToByteBuffer(inputStream, START_HEADER_CRC_LENGTH);
            int nStartHeaderCrc = byteBuffer.getInt();

            byteBuffer = readToByteBuffer(inputStream, START_HEADER_LENGTH);
            CRC crc = new CRC();
            crc.Update(byteBuffer.array());
            long nNextHeaderOffset = byteBuffer.getLong();
            long nNextHeaderSize = byteBuffer.getLong();
            int nNextHeaderCrc = byteBuffer.getInt();

            database.ArchiveInfo.StartPositionAfterHeader = database.ArchiveInfo.StartPosition
                + VERSION_INFO_LENGTH + START_HEADER_CRC_LENGTH + START_HEADER_LENGTH;

            if (nStartHeaderCrc != crc.GetDigest()) {
                throw new ZipException("Header CRC mismatch.");
            }

            if (0 == nNextHeaderSize) {
                return; // no entries
            }

            if (nNextHeaderSize > Integer.MAX_VALUE) {
                throw new ZipException("Invalid header size.");
            }

            skipFully(inputStream, nNextHeaderOffset);

            byteBuffer = readToByteBuffer(inputStream, (int) nNextHeaderSize);
            crc.Init();
            crc.Update(byteBuffer.array());
            if (nNextHeaderCrc != crc.GetDigest()) {
                throw new ZipException("Header CRC mismatch.");
            }



            List<ByteBuffer> dataVector = new LinkedList<ByteBuffer>();

            while (true) {
                long type = readId(byteBuffer);
                if (type == BlockType.HEADER) {
                    break;
                }
                if (type != BlockType.ENCODED_HEADER) {
                    throw new ZipException("Bad block type in header.");
                }
                Reference<Long> startPositionAfterHeaderRef =
                    new Reference<Long>(database.ArchiveInfo.StartPositionAfterHeader);
                Reference<Long> dataStartPosition2Ref =
                    new Reference<Long>(database.ArchiveInfo.DataStartPosition2);
                readAndDecodePackedStreams(byteBuffer, startPositionAfterHeaderRef,
                    dataStartPosition2Ref, dataVector, inputStreamFactory);
                database.ArchiveInfo.StartPositionAfterHeader =
                    startPositionAfterHeaderRef.getValue();
                database.ArchiveInfo.DataStartPosition2 = dataStartPosition2Ref.getValue();
                if (dataVector.isEmpty()) {
                    return;
                }
                if (dataVector.size() > 1) {
                    throw new ZipException("Bad header.");
                }
                byteBuffer = dataVector.remove(0);
            }

            readHeader(byteBuffer, database, inputStreamFactory);
        }

        // 7zIn.cpp
        private void readHeader(ByteBuffer byteBuffer, ArchiveDatabaseEx database,
            InputStreamFactory inputStreamFactory) throws IOException {
            long nBlockType = readId(byteBuffer);

            if (BlockType.ARCHIVE_PROPERTIES == nBlockType) {
                readArchiveProperties(byteBuffer, database.ArchiveInfo);
                nBlockType = readId(byteBuffer);
            }

            List<ByteBuffer> dataVector = new ArrayList<ByteBuffer>();

            if (BlockType.ADDITIONAL_STREAMS_INFO == nBlockType) {
                Reference<Long> startPositionAfterHeaderRef =
                    new Reference<Long>(database.ArchiveInfo.StartPositionAfterHeader);
                Reference<Long> dataStartPosition2Ref =
                    new Reference<Long>(database.ArchiveInfo.DataStartPosition2);
                readAndDecodePackedStreams(byteBuffer, startPositionAfterHeaderRef,
                    dataStartPosition2Ref, dataVector, inputStreamFactory);
                database.ArchiveInfo.StartPositionAfterHeader =
                    startPositionAfterHeaderRef.getValue();
                database.ArchiveInfo.DataStartPosition2 = dataStartPosition2Ref.getValue();
                database.ArchiveInfo.DataStartPosition2 +=
                    database.ArchiveInfo.StartPositionAfterHeader;
                nBlockType = readId(byteBuffer);
            }

            List<Long> unPackSizes = new LinkedList<Long>();
            List<Integer> digests = new LinkedList<Integer>();

            if (BlockType.MAIN_STREAMS_INFO == nBlockType) {
                Reference<Long> dataStartPositionRef =
                    new Reference<Long>(database.ArchiveInfo.DataStartPosition);
                readStreamsInfo(byteBuffer, dataVector, dataStartPositionRef, database.PackSizes,
                    database.PackCRCs, database.Folders, database.NumUnpackStreamsVector,
                    unPackSizes, digests);
                database.ArchiveInfo.DataStartPosition = dataStartPositionRef.getValue();
                nBlockType = readId(byteBuffer);
            } else {
                for (Folder folder : database.Folders) {
                    database.NumUnpackStreamsVector.add(1);
                    unPackSizes.add(folder.getUnpackSize());
                    digests.add(folder.UnpackCRC);
                }
            }

            database.Files.clear();

            if (nBlockType == BlockType.END) {
                return;
            }
            if (nBlockType != BlockType.FILES_INFO) {
                throw new ZipException("Bad block type in header.");
            }

            int numFiles = readNum(byteBuffer);
            int i;
            for (i = 0; i < numFiles; i++) {
                database.Files.add(new FileItem());
            }

            database.ArchiveInfo.FileInfoPopIDs.add((long) BlockType.SIZE);
            if (!database.PackSizes.isEmpty()) {
                database.ArchiveInfo.FileInfoPopIDs.add((long) BlockType.PACK_INFO);
            }
            if (numFiles > 0 && !digests.isEmpty()) {
                database.ArchiveInfo.FileInfoPopIDs.add((long) BlockType.CRC);
            }

            List<Boolean> emptyStreamVector = new ArrayList<Boolean>(numFiles);
            for (i = 0; i < numFiles; i++) {
                emptyStreamVector.add(false);
            }
            List<Boolean> emptyFileVector = new LinkedList<Boolean>();
            List<Boolean> antiFileVector = new LinkedList<Boolean>();
            int numEmptyStreams = 0;

            while (true) {
                long type = readId(byteBuffer);
                if (type == BlockType.END) {
                    break;
                }
                long size = readNumber(byteBuffer);

                database.ArchiveInfo.FileInfoPopIDs.add(type);
                switch ((int) type) {
                    case BlockType.NAME: {
                        ByteBuffer workingByteBuffer = chooseStream(byteBuffer, dataVector);
                        readFileNames(workingByteBuffer, database.Files);
                        break;
                    }
                    case BlockType.WIN_ATTRIBUTES: {
                        List<Boolean> boolVector = new ArrayList<Boolean>(database.Files.size());
                        readBoolVector2(byteBuffer, database.Files.size(), boolVector);
                        ByteBuffer workingByteBuffer = chooseStream(byteBuffer, dataVector);
                        for (i = 0; i < numFiles; i++) {
                            FileItem file = database.Files.get(i);
                            file.AreAttributesDefined = boolVector.get(i);
                            if (file.AreAttributesDefined) {
                                file.Attributes = workingByteBuffer.getInt();
                            }
                        }
                        break;
                    }
                    case BlockType.START_POS: {
                        List<Boolean> boolVector = new ArrayList<Boolean>(database.Files.size());
                        readBoolVector2(byteBuffer, database.Files.size(), boolVector);
                        ByteBuffer workingByteBuffer = chooseStream(byteBuffer, dataVector);
                        for (i = 0; i < numFiles; i++) {
                            FileItem file = database.Files.get(i);
                            file.IsStartPosDefined = boolVector.get(i);
                            if (file.IsStartPosDefined) {
                                file.StartPos = workingByteBuffer.getLong();
                            }
                        }
                        break;
                    }
                    case BlockType.EMPTY_STREAM: {
                        readBoolVector(byteBuffer, numFiles, emptyStreamVector);
                        for (i = 0; i < emptyStreamVector.size(); i++) {
                            if (emptyStreamVector.get(i)) {
                                numEmptyStreams++;
                            }
                        }
                        for (i = 0; i < numEmptyStreams; i++) {
                            emptyFileVector.add(false);
                            antiFileVector.add(false);
                        }
                        break;
                    }
                    case BlockType.EMPTY_FILE: {
                        readBoolVector(byteBuffer, numEmptyStreams, emptyFileVector);
                        break;
                    }
                    case BlockType.ANTI: {
                        readBoolVector(byteBuffer, numEmptyStreams, antiFileVector);
                        break;
                    }
                    case BlockType.CREATION_TIME:
                    case BlockType.LAST_WRITE_TIME:
                    case BlockType.LAST_ACCESS_TIME: {
                        readTime(byteBuffer, dataVector, database.Files, type);
                        break;
                    }
                    default: {
                        database.ArchiveInfo.FileInfoPopIDs
                            .remove(database.ArchiveInfo.FileInfoPopIDs.size() - 1);
                        skipData(byteBuffer, size);
                    }
                }
            }

            int emptyFileIndex = 0;
            int sizeIndex = 0;
            for (i = 0; i < numFiles; i++) {
                FileItem file = database.Files.get(i);
                file.HasStream = !emptyStreamVector.get(i);
                if (file.HasStream) {
                    file.IsDirectory = false;
                    file.IsAnti = false;
                    file.UnPackSize = unPackSizes.get(sizeIndex);
                    file.FileCRC = digests.get(sizeIndex);
                    sizeIndex++;
                } else {
                    file.IsDirectory = !emptyFileVector.get(emptyFileIndex);
                    file.IsAnti = antiFileVector.get(emptyFileIndex);
                    emptyFileIndex++;
                    file.UnPackSize = 0;
                    file.FileCRC = null;
                }
            }
        }

        // 7zIn.cpp
        private void readTime(ByteBuffer byteBuffer, List<ByteBuffer> alternateByteBuffers,
            List<FileItem> files, long type) throws ZipException {
            List<Boolean> boolVector = new ArrayList<Boolean>(files.size());
            readBoolVector2(byteBuffer, files.size(), boolVector);

            byteBuffer = chooseStream(byteBuffer, alternateByteBuffers);

            for (int i = 0; i < files.size(); i++) {
                FileItem file = files.get(i);
                long fileTime = 0;
                boolean defined = boolVector.get(i);
                if (defined) {
                    fileTime = byteBuffer.getLong();
                }
                switch ((int) type) {
                    case BlockType.CREATION_TIME:
                        file.IsCreationTimeDefined = defined;
                        if (defined) {
                            file.CreationTime = fileTime;
                        }
                        break;
                    case BlockType.LAST_WRITE_TIME:
                        file.IsLastWriteTimeDefined = defined;
                        if (defined) {
                            file.LastWriteTime = fileTime;
                        }
                        break;
                    case BlockType.LAST_ACCESS_TIME:
                        file.IsLastAccessTimeDefined = defined;
                        if (defined) {
                            file.LastAccessTime = fileTime;
                        }
                        break;
                }
            }
        }

        // 7zIn.cpp
        private void readFileNames(ByteBuffer byteBuffer, List<FileItem> files) {
            for (FileItem file : files) {
                StringBuilder stringBuilder = new StringBuilder();
                while (true) {
                    char c = byteBuffer.getChar();
                    if (c == '\0') {
                        break;
                    }
                    stringBuilder.append(c);
                }
                file.Name = stringBuilder.toString();
            }
        }

        // 7zIn.cpp
        private void readAndDecodePackedStreams(ByteBuffer byteBuffer, Reference<Long> baseOffset,
            Reference<Long> dataOffset, List<ByteBuffer> dataVector,
            InputStreamFactory inputStreamFactory) throws IOException {
            List<Long> packSizes = new LinkedList<Long>();
            List<Integer> packCRCs = new LinkedList<Integer>();
            List<Folder> folders = new LinkedList<Folder>();

            List<Integer> numUnPackStreamsInFolders = new LinkedList<Integer>();
            List<Long> unPackSizes = new LinkedList<Long>();
            List<Integer> digests = new LinkedList<Integer>();

            readStreamsInfo(byteBuffer, null, dataOffset, packSizes, packCRCs, folders,
                numUnPackStreamsInFolders, unPackSizes, digests);

            int packIndex = 0;
            Decoder decoder = new Decoder();
            long dataStartPos = baseOffset.getValue() + dataOffset.getValue();
            for (int i = 0; i < folders.size(); i++) {
                Folder folder = folders.get(i);

                long unPackSize = folder.getUnpackSize();
                if (unPackSize > NUM_MAX) {
                    throw new ZipException("Bad header.");
                }
                if (unPackSize > Integer.MAX_VALUE) {
                    throw new ZipException("Bad header.");
                }
                ByteArrayOutputStream outStream = new ByteArrayOutputStream((int) unPackSize);

                decoder.Decode(inputStreamFactory, dataStartPos,
                    packSizes.subList(packIndex, packSizes.size()), folder,
                    new SequentialOutStreamWrapper(outStream), null);

                byte[] bytes = outStream.toByteArray();
                if (null != folder.UnpackCRC) {
                    CRC crc = new CRC();
                    crc.Update(bytes);
                    if (folder.UnpackCRC != crc.GetDigest()) {
                        throw new ZipException("Bad header.");
                    }
                }

                dataVector.add(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
                for (int j = 0; j < folder.PackStreams.size(); j++) {
                    dataStartPos += packSizes.get(packIndex++);
                }
            }
        }

        // 7zIn.cpp
        private void readArchiveProperties(ByteBuffer byteBuffer, InArchiveInfo archiveInfo) {
            while (true) {
                long nBlockType = readId(byteBuffer);
                if (BlockType.END == nBlockType) {
                    break;
                }
                skipData(byteBuffer);
            }
        }

        // 7zIn.cpp
        private void readStreamsInfo(ByteBuffer byteBuffer, List<ByteBuffer> alternateByteBuffers,
            Reference<Long> dataOffsetRef,
            List<Long> packedStreamSizes, List<Integer> packedStreamDigests, List<Folder> folders,
            List<Integer> numUnpackStreamsInFolders, List<Long> unpackedStreamSizes,
            List<Integer> digests) throws ZipException {
            while (true) {
                long nBlockType = readId(byteBuffer);
                if (BlockType.END == nBlockType) {
                    return;
                } else if (BlockType.PACK_INFO == nBlockType) {
                    readPackInfo(byteBuffer, dataOffsetRef, packedStreamSizes, packedStreamDigests);
                } else if (BlockType.UNPACK_INFO == nBlockType) {
                    readUnpackInfo(byteBuffer, alternateByteBuffers, folders);
                } else if (BlockType.SUBSTREAMS_INFO == nBlockType) {
                    readSubstreamsInfo(byteBuffer, folders, numUnpackStreamsInFolders,
                        unpackedStreamSizes, digests);
                } else {
                    throw new ZipException("Bad block type in header.");
                }
            }
        }

        // 7zIn.cpp
        private void readSubstreamsInfo(ByteBuffer byteBuffer, List<Folder> folders,
            List<Integer> numUnpackStreamsInFolders, List<Long> unpackedStreamSizes,
            List<Integer> digests) throws ZipException {
            numUnpackStreamsInFolders.clear();
            digests.clear(); // (not in original code)
            long type;
            while (true) {
                type = readId(byteBuffer);
                if (type == BlockType.NUM_UNPACK_STREAM) {
                    for (int i = 0; i < folders.size(); i++) {
                        int value = readNum(byteBuffer);
                        numUnpackStreamsInFolders.add(value);
                    }
                    continue;
                }
                if (type == BlockType.CRC || type == BlockType.SIZE) {
                    break;
                }
                if (type == BlockType.END) {
                    break;
                }
                skipData(byteBuffer);
            }

            if (numUnpackStreamsInFolders.isEmpty()) {
                for (int i = 0; i < folders.size(); i++) {
                    numUnpackStreamsInFolders.add(1);
                }
            }

            int i;
            for (i = 0; i < numUnpackStreamsInFolders.size(); i++) {
                // v3.13 incorrectly worked with empty folders
                // v4.07: we check that folder is empty
                int numSubstreams = numUnpackStreamsInFolders.get(i);
                if (numSubstreams == 0) {
                    continue;
                }
                long sum = 0;
                for (int j = 1; j < numSubstreams; j++) {
                    if (type == BlockType.SIZE) {
                        long size = readNumber(byteBuffer);
                        unpackedStreamSizes.add(size);
                        sum += size;
                    }
                }
                unpackedStreamSizes.add(folders.get(i).getUnpackSize() - sum);
            }
            if (type == BlockType.SIZE) {
                type = readId(byteBuffer);
            }

            int numDigests = 0;
            int numDigestsTotal = 0;
            for (i = 0; i < folders.size(); i++) {
                int numSubstreams = numUnpackStreamsInFolders.get(i);
                if (numSubstreams != 1 || null == folders.get(i).UnpackCRC) {
                    numDigests += numSubstreams;
                }
                numDigestsTotal += numSubstreams;
            }

            while (true) {
                if (type == BlockType.CRC) {
                    List<Integer> digests2 = new ArrayList<Integer>(numDigests);
                    readHashDigests(byteBuffer, numDigests, digests2);
                    int digestIndex = 0;
                    for (i = 0; i < folders.size(); i++) {
                        int numSubstreams = numUnpackStreamsInFolders.get(i);
                        Folder folder = folders.get(i);
                        if (numSubstreams == 1 && null != folder.UnpackCRC) {
                            digests.add(folder.UnpackCRC);
                        } else {
                            for (int j = 0; j < numSubstreams; j++, digestIndex++) {
                                digests.add(digests2.get(digestIndex));
                            }
                        }
                    }
                } else if (type == BlockType.END) {
                    if (digests.isEmpty()) {
                        for (int k = 0; k < numDigestsTotal; k++) {
                            digests.add(null);
                        }
                    }
                    return;
                } else {
                    skipData(byteBuffer);
                }
                type = readId(byteBuffer);
            }
        }

        // 7zIn.cpp
        private void readUnpackInfo(ByteBuffer byteBuffer, List<ByteBuffer> alternateByteBuffers,
            List<Folder> folders) throws ZipException {
            skipToBlockType(byteBuffer, BlockType.FOLDER);
            int numFolders = readNum(byteBuffer);

            ByteBuffer workingByteBuffer = chooseStream(byteBuffer, alternateByteBuffers);
            folders.clear();
            for (int nIndex = 0; nIndex < numFolders; nIndex++) {
                Folder folder = new Folder();
                folders.add(folder);
                readNextFolderItem(workingByteBuffer, folder);
            }

            skipToBlockType(byteBuffer, BlockType.CODERS_UNPACK_SIZE);

            int i;
            for (i = 0; i < numFolders; i++) {
                Folder folder = folders.get(i);
                int numOutStreams = folder.getNumOutStreams();
                for (int j = 0; j < numOutStreams; j++) {
                    long unPackSize = readNumber(byteBuffer);
                    folder.UnpackSizes.add(unPackSize);
                }
            }

            while (true) {
                long type = readId(byteBuffer);
                if (type == BlockType.END) {
                    return;
                }
                if (type == BlockType.CRC) {
                    List<Integer> crcs = new ArrayList<Integer>(numFolders);
                    readHashDigests(byteBuffer, numFolders, crcs);
                    for (i = 0; i < numFolders; i++) {
                        Folder folder = folders.get(i);
                        folder.UnpackCRC = crcs.get(i);
                    }
                    continue;
                }
                skipData(byteBuffer);
            }
        }

        // 7zIn.cpp (GetNextFolderItem)
        private void readNextFolderItem(ByteBuffer byteBuffer, Folder folder) throws ZipException {
            int numCoders = readNum(byteBuffer);

            folder.Coders.clear();
            int numInStreams = 0;
            int numOutStreams = 0;
            int i;
            for (i = 0; i < numCoders; i++) {
                CoderInfo coder = new CoderInfo();
                folder.Coders.add(coder);

                while (true) {
                    AltCoderInfo altCoder = new AltCoderInfo();
                    coder.AltCoders.add(altCoder);
                    byte mainByte = byteBuffer.get();
                    altCoder.MethodID.IDSize = (byte) (mainByte & 0xF);
                    altCoder.MethodID.ID = new byte[altCoder.MethodID.IDSize];
                    byteBuffer.get(altCoder.MethodID.ID);
                    if ((mainByte & 0x10) != 0) {
                        coder.NumInStreams = readNum(byteBuffer);
                        coder.NumOutStreams = readNum(byteBuffer);
                    } else {
                        coder.NumInStreams = 1;
                        coder.NumOutStreams = 1;
                    }
                    if ((mainByte & 0x20) != 0) {
                        int propertiesSize = readNum(byteBuffer);
                        altCoder.Properties = new byte[propertiesSize];
                        byteBuffer.get(altCoder.Properties);
                    }
                    if ((mainByte & 0x80) == 0) {
                        break;
                    }
                }
                numInStreams += coder.NumInStreams;
                numOutStreams += coder.NumOutStreams;
            }

            int numBindPairs = numOutStreams - 1;
            folder.BindPairs.clear();
            for (i = 0; i < numBindPairs; i++) {
                BindPair bindPair = new BindPair();
                bindPair.InIndex = readNum(byteBuffer);
                bindPair.OutIndex = readNum(byteBuffer);
                folder.BindPairs.add(bindPair);
            }

            int numPackedStreams = numInStreams - numBindPairs;
            if (numPackedStreams == 1) {
                for (int j = 0; j < numInStreams; j++) {
                    if (folder.findBindPairForInStream(j) < 0) {
                        folder.PackStreams.add(j);
                        break;
                    }
                }
            } else {
                for (i = 0; i < numPackedStreams; i++) {
                    int packStreamInfo = readNum(byteBuffer);
                    folder.PackStreams.add(packStreamInfo);
                }
            }
        }

        // 7zIn.cpp (CStreamSwitch::Set(CInArchive *, const CObjectVector<CByteBuffer> *)
        private ByteBuffer chooseStream(ByteBuffer byteBuffer,
            List<ByteBuffer> alternateByteBuffers) throws ZipException {
            if (0 == byteBuffer.get()) {
                return byteBuffer;
            } else {
                return alternateByteBuffers.get(readNum(byteBuffer));
            }
        }

        // 7zIn.cpp
        private void readPackInfo(ByteBuffer byteBuffer, Reference<Long> dataOffsetRef,
            List<Long> packSizes, List<Integer> packCRCs) throws ZipException {
            dataOffsetRef.setValue(readNumber(byteBuffer));
            int numPackStreams = readNum(byteBuffer);
            skipToBlockType(byteBuffer, BlockType.SIZE);
            packSizes.clear();
            for (int i = 0; i < numPackStreams; i++) {
                packSizes.add(readNumber(byteBuffer));
            }
            boolean bHasCrcs = false;
            while (true) {
                long nBlockType = readId(byteBuffer);
                if (BlockType.END == nBlockType) {
                    break;
                }
                if (BlockType.CRC == nBlockType) {
                    readHashDigests(byteBuffer, numPackStreams, packCRCs);
                    bHasCrcs = true;
                    continue;
                }
                skipData(byteBuffer);
            }
            if (false == bHasCrcs) {
                packCRCs.clear();
                for (int i = 0; i < numPackStreams; i++) {
                    packCRCs.add(null);
                }
            }
        }

        // 7zIn.cpp
        private void readHashDigests(ByteBuffer byteBuffer, int nItems, List<Integer> digests) {
            digests.clear();
            List<Boolean> digestsDefined = new ArrayList<Boolean>(nItems);
            readBoolVector2(byteBuffer, nItems, digestsDefined);
            for (Boolean isDigestDefined : digestsDefined) {
                if (true == isDigestDefined) {
                    digests.add(byteBuffer.getInt());
                } else {
                    digests.add(null);
                }
            }
        }

        // 7zIn.cpp
        private void readBoolVector(ByteBuffer byteBuffer, int nItems, List<Boolean> booleans) {
            booleans.clear();
            byte nValue = 0;
            int nMask = 0;
            for (int nIndex = 0; nIndex < nItems; nIndex++) {
                if (0 == nMask) {
                    nValue = byteBuffer.get();
                    nMask = 0x80;
                }
                booleans.add(0 != (nValue & nMask));
                nMask >>>= 1;
            }
        }

        // 7zIn.cpp
        private void readBoolVector2(ByteBuffer byteBuffer, int nItems, List<Boolean> booleans) {
            byte nAllAreDefined = byteBuffer.get();
            if (0 == nAllAreDefined) {
                readBoolVector(byteBuffer, nItems, booleans);
            } else {
                booleans.clear();
                for (int nIndex = 0; nIndex < nItems; nIndex++) {
                    booleans.add(true);
                }
            }
        }

        // 7zIn.cpp (WaitAttribute)
        private boolean skipToBlockType(ByteBuffer byteBuffer, long nTargetBlockType) {
            while (true) {
                long nBlockType = readId(byteBuffer);
                if (nTargetBlockType == nBlockType) {
                    return true;
                } else if (BlockType.END == nBlockType) {
                    return false;
                }
                skipData(byteBuffer);
            }
        }

        // 7zIn.cpp (SkeepData)
        private void skipData(ByteBuffer byteBuffer) {
            skipData(byteBuffer, readNumber(byteBuffer));
        }

        // 7zIn.cpp (SkeepData)
        private void skipData(ByteBuffer byteBuffer, long nBytesToSkip) {
            if (nBytesToSkip > Integer.MAX_VALUE) {
                nBytesToSkip = Integer.MAX_VALUE;
            }
            byteBuffer.position(byteBuffer.position() + (int) nBytesToSkip);
        }

        // 7zIn.cpp
        private long readId(ByteBuffer byteBuffer) {
            return readNumber(byteBuffer);
        }

        // 7zIn.cpp
        private int readNum(ByteBuffer byteBuffer) throws ZipException {
            long nValue = readNumber(byteBuffer);
            if (nValue > NUM_MAX) {
                throw new ZipException("Numeric value out of range.");
            }
            return (int) nValue;
        }

        // 7zIn.cpp
        private long readNumber(ByteBuffer byteBuffer) {
            byte nFirstByte = byteBuffer.get();
            int nMask = 0x80;
            long nValue = 0;
            for (int nIndex = 0; nIndex < 8; nIndex++) {
                if (0 == (nFirstByte & nMask)) {
                    long nHighPart = nFirstByte & (nMask - 1);
                    nValue += (nHighPart << (nIndex * 8));
                    break;
                }
                byte b = byteBuffer.get();
                nValue |= ((b & UBYTE_TO_LONG) << (8 * nIndex));
                nMask >>>= 1;
            }
            return nValue;
        }
    }

    // ################################################################
    // Extracting from archive

    private interface ISequentialOutStream {
    }

    private static class SequentialOutStreamWrapper extends Reference<OutputStream>
        implements ISequentialOutStream {
        public SequentialOutStreamWrapper(OutputStream value) {
            super(value);
        }
    }

    private interface ISequentialInStream {
    }

    private static class SequentialInStreamWrapper extends Reference<InputStream>
        implements ISequentialInStream {
        public SequentialInStreamWrapper(InputStream value) {
            super(value);
        }
    }

    private static class CoderMixer2 implements ICompressCoder2 {

        private BindInfoEx m_bindInfo;
        private List<ICompressCoder> m_compressCoders = new LinkedList<ICompressCoder>();
        private List<Long> m_packSizes = new LinkedList<Long>();
        private List<Long> m_unpackSizes = new LinkedList<Long>();

        public void SetBindInfo(BindInfoEx bindInfo) {
            m_bindInfo = bindInfo;
            for (CoderStreamsInfo coderStreamsInfo : m_bindInfo.Coders) {
                if (1 != coderStreamsInfo.NumInStreams || 1 != coderStreamsInfo.NumOutStreams) {
                    Assert.statementNeverExecuted("Not implemented.");
                }
            }
        }

        public void ReInit() {
            m_packSizes.clear();
            m_unpackSizes.clear();
        }

        public void AddCoder(ICompressCoder decoder) {
            m_compressCoders.add(decoder);
        }

        public void SetCoderInfo(int nCoderIndex, List<List<Long>> packSizes,
            List<List<Long>> unpackSizes) {
            Assert.eq(nCoderIndex, "nCoderIndex", m_packSizes.size(), "m_packSizes.size()");
            Assert.eq(packSizes.size(), "packSizes.size()", 1);
            Assert.eq(unpackSizes.size(), "unpackSizes.size()", 1);
            List<Long> packSizesInner = packSizes.get(0);
            List<Long> unpackSizesInner = unpackSizes.get(0);
            Assert.geq(packSizesInner.size(), "packSizesInner.size()", 1);
            Assert.geq(unpackSizesInner.size(), "unpackSizesInner.size()", 1);
            m_packSizes.add(packSizesInner.get(0));
            m_unpackSizes.add(unpackSizesInner.get(0));
        }

        public void Code(ISequentialInStream inStream, ISequentialOutStream outStream, long inSize,
            long outSize, ICompressProgressInfo progress) {
            Assert.statementNeverExecuted();
        }

        public void Code(List<ISequentialInStream> inStreams, List<List<Long>> inSizes,
            int nInStreams, List<ISequentialOutStream> outStreams, List<List<Long>> outSizes,
            int nOutStreams, ICompressProgressInfo progress) throws IOException {
            Assert.eq(inStreams.size(), "inStreams.size()", nInStreams, "nInStreams");
            Assert.eq(outStreams.size(), "outStreams.size()", nOutStreams, "nOutStreams");
            Assert.eq(nOutStreams, "nOutStreams", m_compressCoders.size(),
                "m_compressCoders.size()");
            Assert.eq(nOutStreams, "nOutStreams", nInStreams, "nInStreams");
            Assert.eq(m_compressCoders.size(), "m_compressCoders.size()", m_packSizes.size(),
                "m_packSizes.size()");

            Iterator<ICompressCoder> compressCodersItr = m_compressCoders.iterator();
            Iterator<ISequentialInStream> inStreamsItr = inStreams.iterator();
            Iterator<ISequentialOutStream> outStreamsItr = outStreams.iterator();
            Iterator<Long> packSizesItr = m_packSizes.iterator();
            Iterator<Long> unpackSizesItr = m_unpackSizes.iterator();

            while (compressCodersItr.hasNext()) {
                compressCodersItr.next().Code(inStreamsItr.next(), outStreamsItr.next(),
                    packSizesItr.next(), unpackSizesItr.next(), progress);
            }
        }
    }

    private static class LzmaWrapper implements ICompressCoder, ICompressSetDecoderProperties2 {

        private final SevenZip.Compression.LZMA.Decoder decoder =
            new SevenZip.Compression.LZMA.Decoder();

        public void setDecoderProperties(byte[] properties) throws ZipException {
            if (false == decoder.SetDecoderProperties(properties)) {
                throw new ZipException("Bad decoder properties.");
            }
        }

        public void Code(ISequentialInStream inStream, ISequentialOutStream outStream, long inSize,
            long outSize, ICompressProgressInfo progress) throws IOException {
            if (false == decoder.Code(((Reference<InputStream>) inStream).getValue(),
                ((Reference<OutputStream>) outStream).getValue(), outSize)) {
                throw new ZipException("Bad compressed data.");
            }
        }
    }

    // ICoder.h
    private interface ICompressSetDecoderProperties2 {
        void setDecoderProperties(byte[] properties) throws ZipException;
    }

    // ICoder.h
    private interface ICompressProgressInfo {
        void SetRatioInfo(long inSize, long outSize);
    }

    // ICoder.h
    private interface ICompressCoder {
        void Code(ISequentialInStream inStream, ISequentialOutStream outStream, long inSize,
            long outSize, ICompressProgressInfo progress) throws IOException;
    }

    // ICoder.h
    private interface ICompressCoder2 extends ICompressCoder {
        void Code(List<ISequentialInStream> inStreams, List<List<Long>> inSizes, int nInStreams,
            List<ISequentialOutStream> outStreams, List<List<Long>> outSizes, int nOutStreams,
            ICompressProgressInfo progress) throws IOException;
    }

    // 7zDecode.cpp
    private static final MethodID LZMA_METHOD_ID = new MethodID((byte) 0x3, (byte) 0x1, (byte) 0x1);

    // Archive/Common/CoderMixer2.h
    private static class CoderStreamsInfo {
        int NumInStreams;
        int NumOutStreams;
    }

    // Archive/Common/CoderMixer2.h
    private static class BindInfo {
        List<CoderStreamsInfo> Coders = new LinkedList<CoderStreamsInfo>();
        List<BindPair> BindPairs = new LinkedList<BindPair>();
        List<Integer> InStreams = new LinkedList<Integer>();
        List<Integer> OutStreams = new LinkedList<Integer>();

        void getNumStreams(Reference<Integer> numInStreamsRef,
            Reference<Integer> numOutStreamsRef) {
            int numInStreams = 0;
            int numOutStreams = 0;
            for (CoderStreamsInfo coderStreamsInfo : Coders) {
                numInStreams += coderStreamsInfo.NumInStreams;
                numOutStreams += coderStreamsInfo.NumOutStreams;
            }
            numInStreamsRef.setValue(numInStreams);
            numOutStreamsRef.setValue(numOutStreams);
        }

        int findBinderForInStream(int inStream) {
            for (int i = 0; i < BindPairs.size(); i++) {
                if (BindPairs.get(i).InIndex == inStream) {
                    return i;
                }
            }
            return -1;
        }

        int findBinderForOutStream(int outStream) {
            for (int i = 0; i < BindPairs.size(); i++) {
                if (BindPairs.get(i).OutIndex == outStream) {
                    return i;
                }
            }
            return -1;
        }

        int getCoderInStreamIndex(int coderIndex) {
            int streamIndex = 0;
            for (int i = 0; i < coderIndex; i++) {
                streamIndex += Coders.get(i).NumInStreams;
            }
            return streamIndex;
        }

        int getCoderOutStreamIndex(int coderIndex) {
            int streamIndex = 0;
            for (int i = 0; i < coderIndex; i++) {
                streamIndex += Coders.get(i).NumOutStreams;
            }
            return streamIndex;
        }

        void findInStream(int streamIndex, Reference<Integer> coderIndexRef,
            Reference<Integer> coderStreamIndexRef) throws ZipException {
            int coderIndex;
            int coderStreamIndex;
            for (coderIndex = 0; coderIndex < (int) Coders.size(); coderIndex++) {
                int curSize = Coders.get(coderIndex).NumInStreams;
                if (streamIndex < curSize) {
                    coderStreamIndex = streamIndex;
                    coderStreamIndexRef.setValue(coderStreamIndex);
                    coderIndexRef.setValue(coderIndex);
                    return;
                }
                streamIndex -= curSize;
            }
            throw new ZipException();
        }

        void findOutStream(int streamIndex, Reference<Integer> coderIndexRef,
            Reference<Integer> coderStreamIndexRef) throws ZipException {
            int coderIndex;
            int coderStreamIndex;
            for (coderIndex = 0; coderIndex < (int) Coders.size(); coderIndex++) {
                int curSize = Coders.get(coderIndex).NumOutStreams;
                if (streamIndex < curSize) {
                    coderStreamIndex = streamIndex;
                    coderStreamIndexRef.setValue(coderStreamIndex);
                    coderIndexRef.setValue(coderIndex);
                    return;
                }
                streamIndex -= curSize;
            }
            throw new ZipException();
        }
    }

    // 7zDecode.h
    private static class BindInfoEx extends BindInfo {
        List<MethodID> CoderMethodIDs = new LinkedList<MethodID>();
    }

    // 7zDecode.h,.cpp
    private static class Decoder {

        boolean _bindInfoExPrevIsDefinded;
        BindInfoEx _bindInfoExPrev;

        CoderMixer2 _mixerCoderCommon;
        ICompressCoder2 _mixerCoder;

        List<Object> _decoders = new LinkedList<Object>();

        // 7zDecode.cpp
        public void Decode(InputStreamFactory inputStreamFactory, long startPos,
            List<Long> packSizes, Folder folderInfo, ISequentialOutStream outStream,
            ICompressProgressInfo compressProgress) throws IOException {
            List<ISequentialInStream> inStreams = new LinkedList<ISequentialInStream>();

            try {
                for (int j = 0; j < folderInfo.PackStreams.size(); j++) {
                    InputStream inputStream = inputStreamFactory.createInputStream();
                    skipFully(inputStream, startPos);
                    startPos += packSizes.get(j);

                    LimitedInputStream streamSpec =
                        new LimitedInputStream(inputStream, packSizes.get(j));
                    inStreams.add(new SequentialInStreamWrapper(streamSpec));
                }

                int numCoders = folderInfo.Coders.size();

                BindInfoEx bindInfo = new BindInfoEx();
                ConvertFolderItemInfoToBindInfo(folderInfo, bindInfo);
                boolean createNewCoders;
                if (!_bindInfoExPrevIsDefinded) {
                    createNewCoders = true;
                } else {
                    createNewCoders = !AreBindInfoExEqual(bindInfo, _bindInfoExPrev);
                }
                if (createNewCoders) {
                    int i;
                    _decoders.clear();

                    _mixerCoder = null;

                    {
                        CoderMixer2 coderMixer2 = new CoderMixer2();
                        _mixerCoder = coderMixer2;
                        _mixerCoderCommon = coderMixer2;
                    }

                    _mixerCoderCommon.SetBindInfo(bindInfo);

                    for (i = 0; i < numCoders; i++) {
                        CoderInfo coderInfo = folderInfo.Coders.get(i);
                        AltCoderInfo altCoderInfo = coderInfo.AltCoders.get(0);

                        if (coderInfo.isSimpleCoder()) {
                            ICompressCoder decoder = null;

                            if (altCoderInfo.MethodID.equals(LZMA_METHOD_ID)) {
                                decoder = new LzmaWrapper();
                            }

                            if (null == decoder) {
                                throw new ZipException("Decoder not implemented.");
                            }

                            _decoders.add(decoder);

                            _mixerCoderCommon.AddCoder(decoder);

                        } else {
                            throw new ZipException("Decoder not implemented.");
                        }
                    }
                    _bindInfoExPrev = bindInfo;
                    _bindInfoExPrevIsDefinded = true;
                }
                int i;
                _mixerCoderCommon.ReInit();

                int packStreamIndex = 0, unPackStreamIndex = 0;
                int coderIndex = 0;

                for (i = 0; i < numCoders; i++) {
                    CoderInfo coderInfo = folderInfo.Coders.get(i);
                    AltCoderInfo altCoderInfo = coderInfo.AltCoders.get(0);
                    Object decoder = _decoders.get(coderIndex);

                    if (decoder instanceof ICompressSetDecoderProperties2) {
                        ICompressSetDecoderProperties2 setDecoderProperties =
                            (ICompressSetDecoderProperties2) decoder;

                        byte[] properties = altCoderInfo.Properties;
                        if (null != properties && properties.length > 0) {
                            setDecoderProperties.setDecoderProperties(properties);
                        }
                    }

                    coderIndex++;

                    int numInStreams = coderInfo.NumInStreams;
                    int numOutStreams = coderInfo.NumOutStreams;
                    List<List<Long>> packSizesPointers = new ArrayList<List<Long>>(numInStreams);
                    List<List<Long>> unPackSizesPointers = new ArrayList<List<Long>>(numOutStreams);
                    int j;
                    for (j = 0; j < numOutStreams; j++, unPackStreamIndex++) {
                        unPackSizesPointers.add(folderInfo.UnpackSizes.subList(unPackStreamIndex,
                            folderInfo.UnpackSizes.size()));
                    }

                    for (j = 0; j < numInStreams; j++, packStreamIndex++) {
                        int bindPairIndex = folderInfo.findBindPairForInStream(packStreamIndex);
                        if (bindPairIndex >= 0) {
                            packSizesPointers.add(folderInfo.UnpackSizes.subList(
                                folderInfo.BindPairs.get(bindPairIndex).OutIndex,
                                folderInfo.UnpackSizes.size()));
                        } else {
                            int index = folderInfo.findPackStreamArrayIndex(packStreamIndex);
                            if (index < 0) {
                                throw new ZipException();
                            }
                            packSizesPointers.add(packSizes.subList(index, packSizes.size()));
                        }
                    }

                    _mixerCoderCommon.SetCoderInfo(i, packSizesPointers, unPackSizesPointers);
                }
                // Reference<Integer> mainCoderRef=new Reference<Integer>();
                // Reference<Integer> tempRef=new Reference<Integer>();
                // bindInfo.findOutStream(bindInfo.OutStreams.get(0), mainCoderRef, tempRef);
                // int mainCoder=mainCoderRef.getValue();
                // int temp=tempRef.getValue();
                // _mixerCoderMTSpec.SetProgressCoderIndex(mainCoder); // set which coder in the
                // graph best represents the actual progress of the entire

                if (numCoders == 0) {
                    return;
                }
                List<ISequentialInStream> inStreamPointers =
                    new ArrayList<ISequentialInStream>(inStreams.size());
                for (i = 0; i < inStreams.size(); i++) {
                    inStreamPointers.add(inStreams.get(i));
                }
                List<ISequentialOutStream> outStreamPointer = Collections.singletonList(outStream);
                _mixerCoder.Code(inStreamPointers, null, inStreams.size(), outStreamPointer, null,
                    1, compressProgress);

            } finally {
                for (ISequentialInStream sequentialInStream : inStreams) {
                    ((Reference<InputStream>) sequentialInStream).getValue().close();
                }
            }
        }

        // 7zDecode.cpp
        private static void ConvertFolderItemInfoToBindInfo(Folder folder, BindInfoEx bindInfo) {
            int i;
            for (i = 0; i < folder.BindPairs.size(); i++) {
                BindPair bindPair = new BindPair();
                bindPair.InIndex = folder.BindPairs.get(i).InIndex;
                bindPair.OutIndex = folder.BindPairs.get(i).OutIndex;
                bindInfo.BindPairs.add(bindPair);
            }
            int outStreamIndex = 0;
            for (i = 0; i < folder.Coders.size(); i++) {
                CoderStreamsInfo coderStreamsInfo = new CoderStreamsInfo();
                CoderInfo coderInfo = folder.Coders.get(i);
                coderStreamsInfo.NumInStreams = coderInfo.NumInStreams;
                coderStreamsInfo.NumOutStreams = coderInfo.NumOutStreams;
                bindInfo.Coders.add(coderStreamsInfo);
                AltCoderInfo altCoderInfo = coderInfo.AltCoders.get(0);
                bindInfo.CoderMethodIDs.add(altCoderInfo.MethodID);
                for (int j = 0; j < coderStreamsInfo.NumOutStreams; j++, outStreamIndex++) {
                    if (folder.findBindPairForOutStream(outStreamIndex) < 0) {
                        bindInfo.OutStreams.add(outStreamIndex);
                    }
                }
            }
            for (i = 0; i < folder.PackStreams.size(); i++) {
                bindInfo.InStreams.add((int) folder.PackStreams.get(i));
            }
        }

        // 7zDecode.cpp
        private static boolean AreCodersEqual(CoderStreamsInfo a1, CoderStreamsInfo a2) {
            return (a1.NumInStreams == a2.NumInStreams) && (a1.NumOutStreams == a2.NumOutStreams);
        }

        // 7zDecode.cpp
        static boolean AreBindPairsEqual(BindPair a1, BindPair a2) {
            return (a1.InIndex == a2.InIndex) && (a1.OutIndex == a2.OutIndex);
        }

        // 7zDecode.cpp
        static boolean AreBindInfoExEqual(BindInfoEx a1, BindInfoEx a2) {
            if (a1.Coders.size() != a2.Coders.size()) {
                return false;
            }
            int i;
            for (i = 0; i < a1.Coders.size(); i++) {
                if (!AreCodersEqual(a1.Coders.get(i), a2.Coders.get(i))) {
                    return false;
                }
            }
            if (a1.BindPairs.size() != a2.BindPairs.size()) {
                return false;
            }
            for (i = 0; i < a1.BindPairs.size(); i++) {
                if (!AreBindPairsEqual(a1.BindPairs.get(i), a2.BindPairs.get(i))) {
                    return false;
                }
            }
            for (i = 0; i < a1.CoderMethodIDs.size(); i++) {
                if (a1.CoderMethodIDs.get(i) != a2.CoderMethodIDs.get(i)) {
                    return false;
                }
            }
            if (a1.InStreams.size() != a2.InStreams.size()) {
                return false;
            }
            if (a1.OutStreams.size() != a2.OutStreams.size()) {
                return false;
            }
            return true;
        }
    }

    // 7zExtract.cpp
    private static class ExtractFolderInfo {
        int FileIndex;
        int FolderIndex;
        List<Boolean> ExtractStatuses = new LinkedList<Boolean>();
        long UnPackSize;

        ExtractFolderInfo(int fileIndex, int folderIndex) {
            FileIndex = fileIndex;
            FolderIndex = folderIndex;
            UnPackSize = 0;
            if (fileIndex != NUM_NO_INDEX) {
                ExtractStatuses.add(true);
            }
        }
    }

    // 7zFolderOutStream.h,.cpp
    private static class FolderOutStream implements ISequentialOutStream {

        public void Init(ArchiveDatabaseEx database, int nUnderlyingId, int startIndex,
            List<Boolean> extractStatuses, boolean testMode) {}
    }

    // 7zHandler.h (ported for educational purposes, not used)
    private static class Handler {

        private ArchiveDatabaseEx m_database;

        public void setDatabase(ArchiveDatabaseEx database) {
            m_database = database;
        }

        private InputStreamFactory m_inputStreamFactory;

        public void setInputStreamFactory(InputStreamFactory inputStreamFactory) {
            m_inputStreamFactory = inputStreamFactory;
        }

        // 7zExtract.cpp
        public void Extract(int[] indices, int numItems, boolean testMode) throws IOException {

            boolean allFilesMode = (-1 == numItems);
            if (allFilesMode) {
                numItems = m_database.Files.size();
            }

            if (numItems == 0) {
                return;
            }

            // build up the list of which folders and which streams (files) in those folders to
            // extract
            List<ExtractFolderInfo> extractFolderInfoVector = new LinkedList<ExtractFolderInfo>();
            for (int i = 0; i < numItems; i++) {
                int fileIndex = allFilesMode ? i : indices[i];
                int folderIndex = m_database.FileIndexToFolderIndexMap.get(fileIndex);
                if (NUM_NO_INDEX == folderIndex) {
                    extractFolderInfoVector.add(new ExtractFolderInfo(fileIndex, NUM_NO_INDEX));
                    continue;
                }
                if (extractFolderInfoVector.isEmpty() || folderIndex != extractFolderInfoVector
                    .get(extractFolderInfoVector.size() - 1).FolderIndex) {
                    extractFolderInfoVector.add(new ExtractFolderInfo(NUM_NO_INDEX, folderIndex));
                    Folder folderInfo = m_database.Folders.get(folderIndex);
                    long unPackSize = folderInfo.getUnpackSize();
                    extractFolderInfoVector.get(extractFolderInfoVector.size() - 1).UnPackSize =
                        unPackSize;
                }

                ExtractFolderInfo efi =
                    extractFolderInfoVector.get(extractFolderInfoVector.size() - 1);

                int startIndex = m_database.FolderStartFileIndex.get(folderIndex);
                for (int index = efi.ExtractStatuses.size(); index <= fileIndex
                    - startIndex; index++) {
                    efi.ExtractStatuses.add(index == fileIndex - startIndex);
                }
            }

            Decoder decoder = new Decoder();

            long currentImportantTotalUnPacked = 0;
            long totalFolderUnPacked;

            for (int i = 0; i < extractFolderInfoVector
                .size(); i++, currentImportantTotalUnPacked += totalFolderUnPacked) {
                ExtractFolderInfo efi = extractFolderInfoVector.get(i);
                totalFolderUnPacked = efi.UnPackSize;

                FolderOutStream folderOutStream = new FolderOutStream();

                ArchiveDatabaseEx database = m_database;

                int startIndex;
                if (efi.FileIndex != NUM_NO_INDEX) {
                    startIndex = efi.FileIndex;
                } else {
                    startIndex = database.FolderStartFileIndex.get(efi.FolderIndex);
                }

                folderOutStream.Init(database, 0, startIndex, efi.ExtractStatuses, testMode);

                if (efi.FileIndex != NUM_NO_INDEX) {
                    continue;
                }

                int folderIndex = efi.FolderIndex;
                Folder folderInfo = database.Folders.get(folderIndex);

                int packStreamIndex = database.FolderStartPackStreamIndex.get(folderIndex);
                long folderStartPackPos = database.getFolderStreamPos(folderIndex, 0);

                decoder.Decode(
                    m_inputStreamFactory,
                    folderStartPackPos,
                    database.PackSizes.subList(packStreamIndex, database.PackSizes.size()),
                    folderInfo,
                    folderOutStream,
                    null);
            }
        }
    }

    // ################################################################
    // input stream helper routines

    // ----------------------------------------------------------------
    private static ByteBuffer readToByteBuffer(InputStream inputStream, int nLength)
        throws IOException {
        byte[] bytes = new byte[nLength];
        int nOffset = 0;
        while (nLength > 0) {
            int nBytesRead = inputStream.read(bytes, nOffset, nLength);
            if (-1 == nBytesRead) {
                throw new EOFException();
            } else {
                nOffset += nBytesRead;
                nLength -= nBytesRead;
            }
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    // ----------------------------------------------------------------
    private static void skipFully(InputStream inputStream, long nLength) throws IOException {
        while (nLength > 0) {
            long nSkipped = inputStream.skip(nLength);
            // note: postcodition of skip is wider than that of read! 0 means eof or ...? Let's
            // assume EOF or worse.
            if (nSkipped < 1) {
                throw new EOFException();
            }
            nLength -= nSkipped;
        }
    }

    // ################################################################
    // my extraction routines

    // ----------------------------------------------------------------
    /**
     * Incremental decompressor based on {@link SevenZip.Compression.LZMA.Decoder}.
     */
    private static class LzmaIncrementalDecoder {

        private int m_state;
        private int m_rep0;
        private int m_rep1;
        private int m_rep2;
        private int m_rep3;
        private long m_nowPos64;
        private byte m_prevByte;
        private long m_outSize;
        private boolean m_bEof;

        private static class LenDecoder {

            private short[] m_Choice = new short[2];
            private BitTreeDecoder[] m_LowCoder = new BitTreeDecoder[Base.kNumPosStatesMax];
            private BitTreeDecoder[] m_MidCoder = new BitTreeDecoder[Base.kNumPosStatesMax];
            private BitTreeDecoder m_HighCoder = new BitTreeDecoder(Base.kNumHighLenBits);
            private int m_NumPosStates;

            public void Create(int numPosStates) {
                for (; m_NumPosStates < numPosStates; m_NumPosStates++) {
                    m_LowCoder[m_NumPosStates] = new BitTreeDecoder(Base.kNumLowLenBits);
                    m_MidCoder[m_NumPosStates] = new BitTreeDecoder(Base.kNumMidLenBits);
                }
            }

            public void Init() {
                SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_Choice);
                for (int posState = 0; posState < m_NumPosStates; posState++) {
                    m_LowCoder[posState].Init();
                    m_MidCoder[posState].Init();
                }
                m_HighCoder.Init();
            }

            public int Decode(SevenZip.Compression.RangeCoder.Decoder rangeDecoder, int posState)
                throws IOException {
                if (rangeDecoder.DecodeBit(m_Choice, 0) == 0) {
                    return m_LowCoder[posState].Decode(rangeDecoder);
                }
                int symbol = Base.kNumLowLenSymbols;
                if (rangeDecoder.DecodeBit(m_Choice, 1) == 0) {
                    symbol += m_MidCoder[posState].Decode(rangeDecoder);
                } else {
                    symbol += Base.kNumMidLenSymbols + m_HighCoder.Decode(rangeDecoder);
                }
                return symbol;
            }
        }

        private static class LiteralDecoder {

            private static class Decoder2 {

                private short[] m_Decoders = new short[0x300];

                public void Init() {
                    SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_Decoders);
                }

                public byte DecodeNormal(SevenZip.Compression.RangeCoder.Decoder rangeDecoder)
                    throws IOException {
                    int symbol = 1;
                    do {
                        symbol = (symbol << 1) | rangeDecoder.DecodeBit(m_Decoders, symbol);
                    } while (symbol < 0x100);
                    return (byte) symbol;
                }

                public byte DecodeWithMatchByte(
                    SevenZip.Compression.RangeCoder.Decoder rangeDecoder, byte matchByte)
                    throws IOException {
                    int symbol = 1;
                    do {
                        int matchBit = (matchByte >> 7) & 1;
                        matchByte <<= 1;
                        int bit =
                            rangeDecoder.DecodeBit(m_Decoders, ((1 + matchBit) << 8) + symbol);
                        symbol = (symbol << 1) | bit;
                        if (matchBit != bit) {
                            while (symbol < 0x100) {
                                symbol = (symbol << 1) | rangeDecoder.DecodeBit(m_Decoders, symbol);
                            }
                            break;
                        }
                    } while (symbol < 0x100);
                    return (byte) symbol;
                }
            }

            private LzmaIncrementalDecoder.LiteralDecoder.Decoder2[] m_Coders;
            private int m_NumPrevBits;
            private int m_NumPosBits;
            private int m_PosMask;

            public void Create(int numPosBits, int numPrevBits) {
                if (m_Coders != null && m_NumPrevBits == numPrevBits
                    && m_NumPosBits == numPosBits) {
                    return;
                }
                m_NumPosBits = numPosBits;
                m_PosMask = (1 << numPosBits) - 1;
                m_NumPrevBits = numPrevBits;
                int numStates = 1 << (m_NumPrevBits + m_NumPosBits);
                m_Coders = new LzmaIncrementalDecoder.LiteralDecoder.Decoder2[numStates];
                for (int i = 0; i < numStates; i++) {
                    m_Coders[i] = new LzmaIncrementalDecoder.LiteralDecoder.Decoder2();
                }
            }

            public void Init() {
                int numStates = 1 << (m_NumPrevBits + m_NumPosBits);
                for (int i = 0; i < numStates; i++) {
                    m_Coders[i].Init();
                }
            }

            LzmaIncrementalDecoder.LiteralDecoder.Decoder2 GetDecoder(int pos, byte prevByte) {
                return m_Coders[((pos & m_PosMask) << m_NumPrevBits)
                    + ((prevByte & 0xFF) >>> (8 - m_NumPrevBits))];
            }
        }

        private OutWindow m_OutWindow = new OutWindow();
        private SevenZip.Compression.RangeCoder.Decoder m_RangeDecoder =
            new SevenZip.Compression.RangeCoder.Decoder();

        private short[] m_IsMatchDecoders = new short[Base.kNumStates << Base.kNumPosStatesBitsMax];
        private short[] m_IsRepDecoders = new short[Base.kNumStates];
        private short[] m_IsRepG0Decoders = new short[Base.kNumStates];
        private short[] m_IsRepG1Decoders = new short[Base.kNumStates];
        private short[] m_IsRepG2Decoders = new short[Base.kNumStates];
        private short[] m_IsRep0LongDecoders =
            new short[Base.kNumStates << Base.kNumPosStatesBitsMax];

        private BitTreeDecoder[] m_PosSlotDecoder = new BitTreeDecoder[Base.kNumLenToPosStates];
        private short[] m_PosDecoders = new short[Base.kNumFullDistances - Base.kEndPosModelIndex];

        private BitTreeDecoder m_PosAlignDecoder = new BitTreeDecoder(Base.kNumAlignBits);

        private LzmaIncrementalDecoder.LenDecoder m_LenDecoder =
            new LzmaIncrementalDecoder.LenDecoder();
        private LzmaIncrementalDecoder.LenDecoder m_RepLenDecoder =
            new LzmaIncrementalDecoder.LenDecoder();

        private LzmaIncrementalDecoder.LiteralDecoder m_LiteralDecoder =
            new LzmaIncrementalDecoder.LiteralDecoder();

        private int m_DictionarySize = -1;
        private int m_DictionarySizeCheck = -1;

        private int m_PosStateMask;

        public LzmaIncrementalDecoder(byte[] properties, InputStream inStream,
            OutputStream outStream, long outSize) throws IOException {
            for (int i = 0; i < Base.kNumLenToPosStates; i++) {
                m_PosSlotDecoder[i] = new BitTreeDecoder(Base.kNumPosSlotBits);
            }
            if (!SetDecoderProperties(properties)) {
                throw new ZipException("Invalid properties.");
            }
            Init(inStream, outStream);
            m_outSize = outSize;
        }

        private boolean SetDecoderProperties(byte[] properties) {
            if (properties.length < 5) {
                return false;
            }
            int val = properties[0] & 0xFF;
            int lc = val % 9;
            int remainder = val / 9;
            int lp = remainder % 5;
            int pb = remainder / 5;
            int dictionarySize = 0;
            for (int i = 0; i < 4; i++) {
                dictionarySize += ((int) (properties[1 + i]) & 0xFF) << (i * 8);
            }
            if (!SetLcLpPb(lc, lp, pb)) {
                return false;
            }
            return SetDictionarySize(dictionarySize);
        }

        private boolean SetDictionarySize(int dictionarySize) {
            if (dictionarySize < 0) {
                return false;
            }
            if (m_DictionarySize != dictionarySize) {
                m_DictionarySize = dictionarySize;
                m_DictionarySizeCheck = Math.max(m_DictionarySize, 1);
                m_OutWindow.Create(Math.max(m_DictionarySizeCheck, (1 << 12)));
            }
            return true;
        }

        private boolean SetLcLpPb(int lc, int lp, int pb) {
            if (lc > Base.kNumLitContextBitsMax || lp > 4 || pb > Base.kNumPosStatesBitsMax) {
                return false;
            }
            m_LiteralDecoder.Create(lp, lc);
            int numPosStates = 1 << pb;
            m_LenDecoder.Create(numPosStates);
            m_RepLenDecoder.Create(numPosStates);
            m_PosStateMask = numPosStates - 1;
            return true;
        }

        private void Init(InputStream inStream, OutputStream outStream) throws IOException {
            m_RangeDecoder.SetStream(inStream);
            m_OutWindow.SetStream(outStream);

            m_OutWindow.Init(false);

            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_IsMatchDecoders);
            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_IsRep0LongDecoders);
            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_IsRepDecoders);
            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_IsRepG0Decoders);
            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_IsRepG1Decoders);
            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_IsRepG2Decoders);
            SevenZip.Compression.RangeCoder.Decoder.InitBitModels(m_PosDecoders);

            m_LiteralDecoder.Init();
            for (int i = 0; i < Base.kNumLenToPosStates; i++) {
                m_PosSlotDecoder[i].Init();
            }
            m_LenDecoder.Init();
            m_RepLenDecoder.Init();
            m_PosAlignDecoder.Init();
            m_RangeDecoder.Init();


            m_state = Base.StateInit();
            m_rep0 = 0;
            m_rep1 = 0;
            m_rep2 = 0;
            m_rep3 = 0;

            m_nowPos64 = 0;
            m_prevByte = 0;
        }

        public boolean Code() throws IOException {
            if (m_bEof) {
                throw new EOFException();
            }

            if (m_outSize < 0 || m_nowPos64 < m_outSize) {
                int posState = (int) m_nowPos64 & m_PosStateMask;
                if (m_RangeDecoder.DecodeBit(m_IsMatchDecoders,
                    (m_state << Base.kNumPosStatesBitsMax) + posState) == 0) {
                    LzmaIncrementalDecoder.LiteralDecoder.Decoder2 decoder2 =
                        m_LiteralDecoder.GetDecoder((int) m_nowPos64, m_prevByte);
                    if (!Base.StateIsCharState(m_state)) {
                        m_prevByte = decoder2.DecodeWithMatchByte(m_RangeDecoder,
                            m_OutWindow.GetByte(m_rep0));
                    } else {
                        m_prevByte = decoder2.DecodeNormal(m_RangeDecoder);
                    }
                    m_OutWindow.PutByte(m_prevByte);
                    m_state = Base.StateUpdateChar(m_state);
                    m_nowPos64++;
                } else {
                    int len;
                    if (m_RangeDecoder.DecodeBit(m_IsRepDecoders, m_state) == 1) {
                        len = 0;
                        if (m_RangeDecoder.DecodeBit(m_IsRepG0Decoders, m_state) == 0) {
                            if (m_RangeDecoder.DecodeBit(m_IsRep0LongDecoders,
                                (m_state << Base.kNumPosStatesBitsMax) + posState) == 0) {
                                m_state = Base.StateUpdateShortRep(m_state);
                                len = 1;
                            }
                        } else {
                            int distance;
                            if (m_RangeDecoder.DecodeBit(m_IsRepG1Decoders, m_state) == 0) {
                                distance = m_rep1;
                            } else {
                                if (m_RangeDecoder.DecodeBit(m_IsRepG2Decoders, m_state) == 0) {
                                    distance = m_rep2;
                                } else {
                                    distance = m_rep3;
                                    m_rep3 = m_rep2;
                                }
                                m_rep2 = m_rep1;
                            }
                            m_rep1 = m_rep0;
                            m_rep0 = distance;
                        }
                        if (len == 0) {
                            len = m_RepLenDecoder.Decode(m_RangeDecoder, posState)
                                + Base.kMatchMinLen;
                            m_state = Base.StateUpdateRep(m_state);
                        }
                    } else {
                        m_rep3 = m_rep2;
                        m_rep2 = m_rep1;
                        m_rep1 = m_rep0;
                        len = Base.kMatchMinLen + m_LenDecoder.Decode(m_RangeDecoder, posState);
                        m_state = Base.StateUpdateMatch(m_state);
                        int posSlot =
                            m_PosSlotDecoder[Base.GetLenToPosState(len)].Decode(m_RangeDecoder);
                        if (posSlot >= Base.kStartPosModelIndex) {
                            int numDirectBits = (posSlot >> 1) - 1;
                            m_rep0 = ((2 | (posSlot & 1)) << numDirectBits);
                            if (posSlot < Base.kEndPosModelIndex) {
                                m_rep0 += BitTreeDecoder.ReverseDecode(m_PosDecoders,
                                    m_rep0 - posSlot - 1, m_RangeDecoder, numDirectBits);
                            } else {
                                m_rep0 += (m_RangeDecoder.DecodeDirectBits(
                                    numDirectBits - Base.kNumAlignBits) << Base.kNumAlignBits);
                                m_rep0 += m_PosAlignDecoder.ReverseDecode(m_RangeDecoder);
                                if (m_rep0 < 0) {
                                    if (m_rep0 == -1) {
                                        handleEof();
                                        return true;
                                    }
                                    throw new ZipException("Bad data.");
                                }
                            }
                        } else {
                            m_rep0 = posSlot;
                        }
                    }
                    if (m_rep0 >= m_nowPos64 || m_rep0 >= m_DictionarySizeCheck) {
                        // m_OutWindow.Flush();
                        throw new ZipException("Bad data.");
                    }
                    m_OutWindow.CopyBlock(m_rep0, len);
                    m_nowPos64 += len;
                    m_prevByte = m_OutWindow.GetByte(0);
                }
                return false;
            } else {
                handleEof();
                return true;
            }
        }

        private void handleEof() throws IOException {
            m_bEof = true;
            m_OutWindow.Flush();
            m_OutWindow.ReleaseStream();
            m_RangeDecoder.ReleaseStream();
        }
    }

    // ----------------------------------------------------------------
    private static class LzmaDecompressingInputStream extends InputStream {

        private InputStream m_inputStream;
        private LzmaIncrementalDecoder m_decoder;
        private ByteArrayOutputStream m_byteArrayOutputStream;
        private byte[] m_bytes;
        private int m_nBytesRemaining;
        private boolean m_bEof;
        private byte[] m_singleByteBuf = new byte[1];

        public LzmaDecompressingInputStream(InputStreamFactory inputStreamFactory,
            long nOffsetIntoArchive, long nPackedSize, long nUnpackedSize, byte[] properties)
            throws IOException {
            InputStream inputStream = inputStreamFactory.createInputStream();
            skipFully(inputStream, nOffsetIntoArchive);
            m_inputStream = new LimitedInputStream(inputStream, nPackedSize);
            m_byteArrayOutputStream = new ByteArrayOutputStream();
            m_decoder = new LzmaIncrementalDecoder(properties, m_inputStream,
                m_byteArrayOutputStream, nUnpackedSize);
        }

        // skip, available, markSupported, mark, reset - all stubbed out by base class

        // delegate to read(byte[], int , int)
        @Override
        public int read() throws IOException {
            return -1 == read(m_singleByteBuf, 0, 1) ? -1 : m_singleByteBuf[0] & UBYTE_TO_INT;
        }

        @Override
        public void close() throws IOException {
            if (null != m_inputStream) {
                m_decoder = null;
                m_byteArrayOutputStream = null;
                m_bytes = null;
                InputStream inputStream = m_inputStream;
                m_inputStream = null;
                inputStream.close();
            }
        }

        @Override
        public int read(byte[] bytes, int nOffset, int nLength) throws IOException {
            if (null == m_inputStream) {
                throw new IOException("Stream closed.");
            }

            if (nOffset < 0 || nLength < 0 || nOffset > bytes.length - nLength) {
                throw new IndexOutOfBoundsException();
            } else if (0 == nLength) {
                return 0;
            }

            while (true) {
                if (m_byteArrayOutputStream.size() > 0) {
                    // pull data out of the buffer
                    m_bytes = m_byteArrayOutputStream.toByteArray();
                    m_byteArrayOutputStream.reset();
                    m_nBytesRemaining = m_bytes.length;
                }
                if (null != m_bytes) {
                    // return what we've got
                    nLength = Math.min(nLength, m_nBytesRemaining);
                    System.arraycopy(m_bytes, m_bytes.length - m_nBytesRemaining, bytes, nOffset,
                        nLength);
                    m_nBytesRemaining -= nLength;
                    if (0 == m_nBytesRemaining) {
                        m_bytes = null;
                    }
                    return nLength;
                }
                if (m_bEof) {
                    // no more
                    return -1;
                } else {
                    // run the decompression algorithm for a while
                    m_bEof = m_decoder.Code();
                }
            }
        }
    }

    // ----------------------------------------------------------------
    private static class LimitedInputStream extends InputStream {

        enum CloseUnderlyingOnClose {
            YES, NO
        }

        private InputStream m_inputStream;
        private long m_nBytesRemaining;
        private final CloseUnderlyingOnClose m_closeUnderlyingOnClose;

        public LimitedInputStream(InputStream inputStream, long nBytes) {
            this(inputStream, nBytes, CloseUnderlyingOnClose.YES);
        }

        public LimitedInputStream(InputStream inputStream, long nBytes,
            CloseUnderlyingOnClose closeUnderlyingOnClose) {
            m_inputStream = inputStream;
            m_nBytesRemaining = nBytes;
            m_closeUnderlyingOnClose = closeUnderlyingOnClose;
        }

        // available, markSupported, mark, reset - all stubbed out by base class

        @Override
        public int read() throws IOException {
            if (null == m_inputStream) {
                throw new IOException("Stream closed.");
            } else if (0 == m_nBytesRemaining) {
                return -1;
            }

            int nValue = m_inputStream.read();
            if (-1 != nValue) {
                m_nBytesRemaining--;
            }
            return nValue;
        }

        @Override
        public long skip(long nLength) throws IOException {
            if (null == m_inputStream) {
                throw new IOException("Stream closed.");
            } else if (0 == m_nBytesRemaining) {
                return -1;
            }

            nLength = Math.min(nLength, m_nBytesRemaining);
            long nSkipped = m_inputStream.skip(nLength);
            if (nSkipped > 0) {
                m_nBytesRemaining -= nSkipped;
            }
            return nSkipped;
        }

        public void skipToEnd() throws IOException {
            skipFully(this, m_nBytesRemaining);
        }

        @Override
        public int read(byte[] bytes, int nOffset, int nLength) throws IOException {
            if (null == m_inputStream) {
                throw new IOException("Stream closed.");
            }
            if (nOffset < 0 || nLength < 0 || nOffset > bytes.length - nLength) {
                throw new IndexOutOfBoundsException();
            } else if (0 == nLength) {
                return 0;
            }
            if (0 == m_nBytesRemaining) {
                return -1;
            }
            nLength = (int) Math.min(nLength, m_nBytesRemaining);

            int nRead = m_inputStream.read(bytes, nOffset, nLength);
            if (nRead > 0) {
                m_nBytesRemaining -= nRead;
            }
            return nRead;
        }

        @Override
        public void close() throws IOException {
            if (null != m_inputStream) {
                InputStream inputStream = m_inputStream;
                m_inputStream = null;
                if (CloseUnderlyingOnClose.YES == m_closeUnderlyingOnClose) {
                    inputStream.close();
                }
            }
        }
    }

    // ----------------------------------------------------------------
    private static class ArchiveIterator {

        private Iterator<FileItem> m_fileItr;
        private Iterator<Folder> m_folderItr;
        private Iterator<Integer> m_unpackStreamsForFolderItr;
        private Iterator<Long> m_packStreamSizesItr;

        private long m_nOffsetIntoArchive;
        private int m_nStreamsRemainingInFolder;

        // ------------------------------------------------------------
        public ArchiveIterator(ArchiveDatabaseEx archiveDatabaseEx) {
            m_fileItr = archiveDatabaseEx.Files.iterator();
            m_folderItr = archiveDatabaseEx.Folders.iterator();
            m_unpackStreamsForFolderItr = archiveDatabaseEx.NumUnpackStreamsVector.iterator();
            m_packStreamSizesItr = archiveDatabaseEx.PackSizes.iterator();
            m_nOffsetIntoArchive = archiveDatabaseEx.ArchiveInfo.DataStartPosition
                + archiveDatabaseEx.ArchiveInfo.StartPositionAfterHeader;
        }
    }

    // ################################################################
    // public inner classes

    // ----------------------------------------------------------------
    public static class Entry {
        private final FileItem m_fileItem;

        protected Entry(FileItem fileItem) {
            m_fileItem = fileItem;
        }

        public String getName() {
            return m_fileItem.Name;
        }

        public boolean isDirectory() {
            return m_fileItem.IsDirectory;
        }
    }

    public enum Behavior {
        SKIP_WHEN_NO_STREAM, INCLUDE_WHEN_NO_STREAM
    }

    // ################################################################
    // ZipInputStream impl

    private final InputStreamFactory m_inputStreamFactory;
    private boolean m_bIsClosed;
    private ArchiveIterator m_archiveIterator;
    private InputStream m_currentFolderStream;
    private LimitedInputStream m_currentFileStream;

    private byte[] m_singleByteBuf = new byte[1];

    // ----------------------------------------------------------------
    public SevenZipInputStream(InputStreamFactory inputStreamFactory) {
        Require.neqNull(inputStreamFactory, "inputStreamFactory");
        m_inputStreamFactory = inputStreamFactory;
    }

    // ----------------------------------------------------------------
    public Entry getNextEntry(Behavior behavior) throws IOException {
        verifyIsOpen();

        // noinspection OverlyBroadCatchBlock
        try {
            // make sure we have read the archive database
            if (null == m_archiveIterator) {
                ArchiveDatabaseEx archiveDatabaseEx = readArchiveDatabase();
                m_archiveIterator = new ArchiveIterator(archiveDatabaseEx);
            }

            // find next file with a stream (ie, skip directories)
            FileItem fileItem;
            while (true) {
                if (false == m_archiveIterator.m_fileItr.hasNext()) {
                    closeAllStreams(); // no more reading allowed
                    return null;
                }
                fileItem = m_archiveIterator.m_fileItr.next();
                if (true == fileItem.HasStream) {
                    break;
                } else if (Behavior.SKIP_WHEN_NO_STREAM == behavior) {
                    continue;
                }

                // when asked, return an entry for directories but make sure there is no stream
                if (null != m_currentFileStream) {
                    LimitedInputStream currentFileStream = m_currentFileStream;
                    m_currentFileStream = null;
                    currentFileStream.close();
                }
                return new Entry(fileItem);
            }

            if (0 == m_archiveIterator.m_nStreamsRemainingInFolder) {
                closeAllStreams(); // throw out whatever we've got because we will start with new
                                   // streams

                // open next folder-stream
                Folder folder;
                do {
                    if (false == m_archiveIterator.m_folderItr.hasNext()
                        || false == m_archiveIterator.m_unpackStreamsForFolderItr.hasNext()) {
                        throw new ZipException("Bad header.");
                    }
                    folder = m_archiveIterator.m_folderItr.next();
                    m_archiveIterator.m_nStreamsRemainingInFolder =
                        m_archiveIterator.m_unpackStreamsForFolderItr.next();
                } while (0 == m_archiveIterator.m_nStreamsRemainingInFolder);

                if (folder.Coders.isEmpty()) {
                    throw new ZipException("Bad header.");
                } else if (folder.Coders.size() > 1) {
                    throw new ZipException("Unsupported compression type.");
                }
                CoderInfo coderInfo = folder.Coders.get(0);
                if (false == coderInfo.isSimpleCoder()) {
                    throw new ZipException("Unsupported compression type.");
                }
                if (coderInfo.AltCoders.isEmpty()) {
                    throw new ZipException("Bad header.");
                }
                AltCoderInfo altCoderInfo = coderInfo.AltCoders.get(0);
                if (!LZMA_METHOD_ID.equals(altCoderInfo.MethodID)) {
                    throw new ZipException("Unsupported compression type.");
                }
                byte[] properties = altCoderInfo.Properties;
                long nUnpackedSize = folder.getUnpackSize();

                long nPackedSize = m_archiveIterator.m_packStreamSizesItr.next();
                long nOffsetIntoArchive = m_archiveIterator.m_nOffsetIntoArchive;
                m_archiveIterator.m_nOffsetIntoArchive += nPackedSize;

                m_currentFolderStream = new LzmaDecompressingInputStream(m_inputStreamFactory,
                    nOffsetIntoArchive, nPackedSize, nUnpackedSize, properties);
            }

            // if we are in the middle of a folder-stream, skip to the end
            // of the current file so we are positioned correctly for the next file
            if (null != m_currentFileStream) {
                m_currentFileStream.skipToEnd();
                m_currentFileStream.close();
                m_currentFileStream = null;
            }

            // get next file-stream from folder-stream
            m_archiveIterator.m_nStreamsRemainingInFolder--;
            m_currentFileStream = new LimitedInputStream(m_currentFolderStream, fileItem.UnPackSize,
                LimitedInputStream.CloseUnderlyingOnClose.NO);
            return new Entry(fileItem);

        } catch (IOException e) {
            try {
                closeAllStreams();
            } catch (IOException e2) {
                /* ignore */ }
            throw e;
        }
    }

    // ----------------------------------------------------------------
    private ArchiveDatabaseEx readArchiveDatabase() throws IOException {
        ArchiveDatabaseEx archiveDatabaseEx = new ArchiveDatabaseEx();

        // read all the index info out of the file
        InputStream inputStream = m_inputStreamFactory.createInputStream();
        try {
            // read the header
            byte[] signature = readToByteBuffer(inputStream, SIGNATURE_LENGTH).array();
            if (false == Arrays.equals(signature, SIGNATURE)) {
                throw new ZipException("Bad file signature.");
            }

            // read the rest
            new InArchive().readDatabase(inputStream, archiveDatabaseEx, m_inputStreamFactory,
                SIGNATURE_LENGTH);
        } finally {
            inputStream.close();
        }

        archiveDatabaseEx.fill();
        return archiveDatabaseEx;
    }

    // ----------------------------------------------------------------
    private void verifyIsOpen() throws IOException {
        if (m_bIsClosed) {
            throw new IOException("Stream closed.");
        }
    }

    // ################################################################
    // from InputStream

    // skip, available, markSupported, mark, reset - all stubbed out by base class

    // ----------------------------------------------------------------
    // delegate to read(byte[], int , int)
    @Override
    public int read() throws IOException {
        return -1 == read(m_singleByteBuf, 0, 1) ? -1 : m_singleByteBuf[0] & UBYTE_TO_INT;
    }

    // ----------------------------------------------------------------
    // this is the main read method
    @Override
    public int read(byte[] bytes, int nOffset, int nLength) throws IOException {
        verifyIsOpen();
        if (nOffset < 0 || nLength < 0 || nOffset > bytes.length - nLength) {
            throw new IndexOutOfBoundsException();
        } else if (0 == nLength) {
            return 0;
        }

        if (null == m_currentFileStream) {
            return -1;
        }

        return m_currentFileStream.read(bytes, nOffset, nLength);
    }

    // ----------------------------------------------------------------
    @Override
    public void close() throws IOException {
        if (false == m_bIsClosed) {
            m_bIsClosed = true;
            closeAllStreams();
        }
    }

    // ----------------------------------------------------------------
    private void closeAllStreams() throws IOException {
        LimitedInputStream currentFileStream = m_currentFileStream;
        m_currentFileStream = null;
        InputStream currentFolderDecompressedStream = m_currentFolderStream;
        m_currentFolderStream = null;

        if (null != currentFileStream) {
            currentFileStream.close();
        }
        if (null != currentFolderDecompressedStream) {
            currentFolderDecompressedStream.close();
        }
    }

}
