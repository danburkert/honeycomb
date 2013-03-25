#ifndef SERIALIZABLE_H

#define SERIALIZABLE_H

class Serializable
{
  public:
    Serializable() {}
    virtual ~Serializable() = 0;
    virtual int serialize(const char** buf, size_t* len) = 0;

    virtual int deserialize(const char* buf, int64_t len) = 0;
};

#endif /* end of include guard: SERIALIZABLE_H */
