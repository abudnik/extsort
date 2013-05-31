#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <vector>
#include <algorithm>

using namespace std;

static const int BLOCK_SIZE = 256 * 1024 * 1024; // 256 Mb
static const int OUTPUT_BUFFER_SIZE = 32 * 1024 * 1024; // 32 Mb

struct ChunkRef
{
    int offset;
    int length;
};

struct CompareRefs
{
    CompareRefs(const char *buf) : buffer(buf) {}

    // a < b
    bool operator() (const ChunkRef &a, const ChunkRef &b) const
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
    Sorter( istream &is_, ostream &os_, char *buffer_ ) : is(is_), os(os_), buffer(buffer_) {}

    void Sort()
    {
        int chunk = SortChunks();
        MergeChunks( chunk );
    }

private:
    int SortChunks()
    {
        uint64_t file_offset = 0;
        int tail_reminder;
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

    int ParseChunk()
    {
        int i, offset;
        ChunkRef ref;

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

        // get uncomplete number length in buffer (reminder)
        if ( buffer[BLOCK_SIZE - 1] != '\n' )
        {
            i = refs.back().offset + refs.back().length + 1;
            return BLOCK_SIZE - i;
        }

        return 0;
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

        for( unsigned int i = 0; i < refs.size(); ++i )
        {
            o.write( buffer + refs[i].offset, refs[i].length );
            o << endl;
        }
    }

protected:
    istream &is;
    ostream &os;
    char *buffer;
    vector<ChunkRef> refs;
};

class MultiPhaseMergeSorter : public Sorter
{
    struct BufferRef
    {
        int offset;
        int length;
        char *buffer;
    };

    struct MergeBuffer
    {
        char *buffer;
        vector<BufferRef> refs;
        int current_ref;
        uint64_t file_offset;
        bool skip;
    };

public:
    MultiPhaseMergeSorter( istream &is_, ostream &os_, char *buffer_ )
    : Sorter( is_, os_, buffer_ )
    {
        output_buffer = new char[ OUTPUT_BUFFER_SIZE ];
        output_offset = 0;
    }

    ~MultiPhaseMergeSorter()
    {
        delete[] output_buffer;
    }

private:
    virtual void MergeChunks( int chunk )
    {
        num_buffers = chunk;
        AllocMergeBuffers();

        BufferRef min;

        while( 1 )
        {
            FillEmptyBuffers();
            if ( !FindMinimumValue( min ) )
                break;
            PushToOutput( min );
        }

        FreeMergeBuffers();
    }

    bool FindMinimumValue( BufferRef &min )
    {
        bool found_first = false;
        int last_min;

        for( int i = 0; i < num_buffers; ++i )
        {
            if ( !buffers[i].skip )
            {
                min = buffers[i].refs[ buffers[i].current_ref ];
                found_first = true;
                last_min = i;
                break;
            }
        }
        if ( !found_first )
            return false;

        for( int i = last_min; i < num_buffers; ++i )
        {
            if ( !buffers[i].skip )
            {
                if ( CmpLess( buffers[i].refs[ buffers[i].current_ref ], min ) )
                {
                    min = buffers[i].refs[ buffers[i].current_ref ];
                    last_min = i;
                }
            }
        }

        ++buffers[ last_min ].current_ref;
        return true;
    }

    // a < b
    bool CmpLess( const BufferRef &a, const BufferRef &b ) const
    {
        if (a.length != b.length)
            return a.length < b.length;

        return strncmp(a.buffer + a.offset, b.buffer + b.offset, a.length) < 0;
    }

    void PushToOutput( const BufferRef &min )
    {
        if ( output_offset + min.length + 1 >= OUTPUT_BUFFER_SIZE )
        {
            // flush buffer to disk
            os.write( output_buffer, output_offset );
            output_offset = 0;
        }

        strncpy( output_buffer + output_offset, min.buffer + min.offset, min.length );
        output_buffer[output_offset + min.length] = '\n';
        output_offset += min.length + 1;
    }

    void FillEmptyBuffers()
    {
        for( int i = 0; i < num_buffers; ++i )
        {
            if ( buffers[i].current_ref >= buffers[i].refs.size() && !buffers[i].skip ) // check if buffer is empty
            {
                if ( buffers[i].file_offset >= BLOCK_SIZE ) // chunk were parsed already
                {
                    buffers[i].skip = true;
                    continue;
                }

                ostringstream ss;
                ss << "chunk/" << i;
                ifstream is( ss.str().c_str() );

                is.seekg( buffers[i].file_offset, is.beg );
                is.read( buffers[i].buffer, buffer_size );

                int tail_reminder = ParseBuffer( buffers[i] );
                buffers[i].file_offset += buffer_size - tail_reminder;

                cout << "read buffer of chunk = " << i << endl;
            }
        }
    }

    int ParseBuffer( MergeBuffer &b ) const
    {
        int i, offset;
        BufferRef ref;

        b.refs.clear();
        b.current_ref = 0;

        offset = 0;
        for( i = 0; i < buffer_size; ++i )
        {
            if ( b.buffer[i] == '\n' )
            {
                ref.length = i - offset;
                ref.offset = offset;
                ref.buffer = b.buffer;
                offset = i + 1;
                b.refs.push_back( ref );
            }
        }

        if ( b.buffer[buffer_size - 1] != '\n' )
        {
            i = b.refs.back().offset + b.refs.back().length + 1;
            return buffer_size - i;
        }

        return 0;
    }

    void AllocMergeBuffers()
    {
        // use merge buffers on top of buffer
        buffers = new MergeBuffer[num_buffers];
        buffer_size = BLOCK_SIZE / num_buffers;

        for( int i = 0; i < num_buffers; ++i )
        {
            buffers[i].buffer = Sorter::buffer + i * buffer_size;
            buffers[i].current_ref = 0;
            buffers[i].file_offset = 0;
            buffers[i].skip = false;
        }
    }

    void FreeMergeBuffers()
    {
        delete[] buffers;
    }

private:
    MergeBuffer *buffers; // small buffer for each chunk
    int num_buffers;
    int buffer_size;
    char *output_buffer;
    int output_offset;
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
