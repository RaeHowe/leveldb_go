package internal

const (
	//Level-0 文件数量超过 4 个，合并入 Level-1；
	L0_CompactionTrigger = 4
	//Level-0 文件数量超过 8 个，则会延迟 1ms 再写入键值对，减慢写入速度；
	L0_SlowdownWritesTrigger = 8
	//Level-0 文件数量超过 12 个，停止写入。
	//.... c++代码有，此处没有

	//Write_buffer_size        = 4 << 20 // 4MB
	Write_buffer_size     = 4 << 7 // 为了实验此处小点
	NumLevels             = 7      //sst一共层数
	MaxOpenFiles          = 1000
	NumNonTableCacheFiles = 10
	MaxMemCompactLevel    = 0
	//MaxFileSize              = 2 << 20 // 2MB
	MaxFileSize = 2 << 6 //

	BaseLevelSize = 2 << 8
)
