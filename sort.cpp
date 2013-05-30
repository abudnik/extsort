#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <vector>
#include <algorithm>

using namespace std;

static const int BLOCK_SIZE = 256 * 1024 * 1024; // 256 Mb

struct BufferRef
{
    int offset;
    int length;
};

struct CompareRefs
{
    CompareRefs(const char *buf) : buffer(buf) {}

    // a < b
    bool operator() (const BufferRef &a, const BufferRef &b) const
    {
        if (a.length != b.length)
            return a.length < b.length;

        return strncmp(buffer + a.offset, buffer + b.offset, a.length) < 0;
    }

private:
    const char *buffer;
};

class Sorter
{
public:
    Sorter( istream &is_, ostream &os_, char *buffer_ ) : is(is_), os(os_), buffer(buffer_)
    {
    }

    void Sort()
    {
        MergeChunks( SortChunks() );
    }

private:
    int SortChunks()
    {
        uint64_t file_offset = 0;
        uint64_t tail_reminder;
        int chunk = 0;

        while( is.good() )
        {
            is.seekg( file_offset, is.beg );
            is.read( buffer, BLOCK_SIZE );
            cout << "chunk " << chunk << " readed" << endl;

            tail_reminder = ParseChunk();
            cout << "chunk " << chunk << " parsed" << endl;
            file_offset += BLOCK_SIZE - tail_reminder;

            SortRefs();
            cout << "chunk " << chunk << " sorted" << endl;

            SaveSortedChunk( chunk );
            cout << "chunk " << chunk << " saved" << endl;

            ++chunk;
        }

        return chunk;
    }

    virtual void MergeChunks( int chunk ) = 0;

    uint64_t ParseChunk()
    {
        int i, offset;
        BufferRef ref;

        refs.clear();

        offset = 0;
        for( i = 0; i < BLOCK_SIZE; ++i )
        {
            if ( buffer[i] == '\n' )
            {
                ref.length = i - offset;
                ref.offset = offset;
                offset = i + 1;
                refs.push_back( ref );
            }
        }

        return (uint64_t)(BLOCK_SIZE - i);
    }

    void SortRefs()
    {
        CompareRefs cmp( buffer );
        std::sort( refs.begin(), refs.end(), cmp );
    }
    
    void SaveSortedChunk( int chunk ) const
    {
        ostringstream ss;
        ss << "chunk/" << chunk;
        ofstream o( ss.str().c_str() );

        for( int i = 0; i < refs.size(); ++i )
        {
            o.write( buffer + refs[i].offset, refs[i].length );
            o << endl;
        }
    }

protected:
    istream &is;
    ostream &os;
    char *buffer;
    vector<BufferRef> refs;
};

class MultiPhaseMergeSorter : public Sorter
{
    struct MergeBuffer
    {
        char *buffer;
        vector<BufferRef> refs;
        int current;
    };

public:
    MultiPhaseMergeSorter( istream &is_, ostream &os_, char *buffer_ )
    : Sorter( is_, os_, buffer_ ) {}

private:
    virtual void MergeChunks( int chunk )
    {
        num_buffers = chunk;
        AllocMergeBuffers();

        bool sorted = false;
        while( !sorted )
        {
            FillEmptyBuffers();

            sorted = true;
        }

        FreeMergeBuffers();
    }

    void FillEmptyBuffers()
    {
        for( int i = 0; i < num_buffers; ++i )
        {
            if ( buffers[i].current >= buffers[i].refs.size() )
            {
                
            }
        }
    }

    void AllocMergeBuffers()
    {
        // use merge buffers on top of buffer
        buffers = new MergeBuffer[num_buffers];
        int buffer_size = BLOCK_SIZE / num_buffers;

        for( int i = 0; i < num_buffers; ++i )
        {
            buffers[i].buffer = Sorter::buffer + i * buffer_size;
            buffers[i].current = 0;
        }
    }

    void FreeMergeBuffers()
    {
        delete[] buffers;
    }

private:
    MergeBuffer *buffers; // small buffer for each chunk
    int num_buffers;
};


int main()
{
    cout << "sorting started..." << endl;

    char *buffer = new(std::nothrow) char[ BLOCK_SIZE ];
    if (!buffer)
    {
        cout << "malloc failed" << endl;
        return 1;
    }

    ifstream input( "input.txt" );
    ofstream output( "output.txt" );

    MultiPhaseMergeSorter sorter( input, output, buffer );
    sorter.Sort();

    delete[] buffer;
    buffer = NULL;

    cout << "sorting finished..." << endl;
    return 0;
}
