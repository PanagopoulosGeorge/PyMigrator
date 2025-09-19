"""
https://www.independent-software.com/dbase-dbf-dbt-file-format.html


https://blogs.embarcadero.com/dbase-dbf-file-structure/

DBVERSION_STRINGS = {
    0x02: 'FoxBASE',
    0x03: 'FoxBASE+/Dbase III plus, no memory',
    0x30: 'Visual FoxPro',
    0x31: 'Visual FoxPro, autoincrement enabled',
    0x32: 'Visual FoxPro with field type Varchar or Varbinary',
    0x43: 'dBASE IV SQL table files, no memo',
    0x63: 'dBASE IV SQL system files, no memo',
    0x83: 'FoxBASE+/dBASE III PLUS, with memo',
    0x8B: 'dBASE IV with memo',                            <=====================
    0xCB: 'dBASE IV SQL table files, with memo',
    0xF5: 'FoxPro 2.x (or earlier) with memo',
    0xE5: 'HiPer-Six format with SMT memo file',
    0xFB: 'FoxBASE',

Byte 0
-----------
x xxx x 001 = 0x?1 not used
0 000 0 010 = 0x02 FoxBASE
0 000 0 011 = 0x03 FoxBASE+/dBASE III PLUS, no memo
x xxx x 100 = 0x?4 dBASE 7
0 000 0 101 = 0x05 dBASE 5, no memo
0 011 0 000 = 0x30 Visual FoxPro
0 011 0 001 = 0x31 Visual FoxPro, autoincrement enabled
0 011 0 010 = 0x32 Visual FoxPro, Varchar, Varbinary, or Blob-enabled
0 100 0 011 = 0x43 dBASE IV SQL table files, no memo
0 110 0 011 = 0x63 dBASE IV SQL system files, no memo
0 111 1 011 = 0x7B dBASE IV, with memo
1 000 0 011 = 0x83 FoxBASE+/dBASE III PLUS, with memo
1 000 1 011 = 0x8B dBASE IV, with memo                     <=====================
1 000 1 110 = 0x8E dBASE IV with SQL table
1 100 1 011 = 0xCB dBASE IV SQL table files, with memo
1 110 0 101 = 0xE5 Clipper SIX driver, with SMT memo
1 111 0 101 = 0xF5 FoxPro 2.x (or earlier) with memo
1 111 1 011 = 0xFB FoxBASE (with memo?)
| ||| | |||
| ||| | |||   Bit flags (not used in all formats)
| ||| | |||   -----------------------------------
| ||| | +++-- bits 2, 1, 0, version (x03 = level 5, x04 = level 7)
| ||| +------ bit 3, presence of memo file
| +++-------- bits 6, 5, 4, presence of dBASE IV SQL table
+------------ bit 7, presence of .DBT file

"""

import os
import struct
import bitstring
import datetime

class ParseDBFb:
    def __init__(self, path, encoding):
        self.path = path
        self.encoding = encoding
        self.memo_block_size = 512 # Default value
        self.memo_field_exists = False
        self.memo_biggest_size = 0
        self.memo_block_number_size = 10
        self.memofp = None
        
        self.openDBF()
        if self.buffer is not None and len(self.buffer) > 0:
            self.parseDBFInfo()
            self.parseDBFMetadata()
            if self.memo_field_exists:
                self.openDBT()
            self.parseDBFData()
            if self.memofp is not None:
                self.memofp.close()
    
    def openDBF(self):
        self.buffer = bytes([])
        try:
            with open(self.path, 'rb') as fp:
                self.buffer = fp.read()
        except Exception as err:
            print(str(err))
        
    def openDBT(self):
        try:
            self.memofp = open(self.path.replace('.DBF','.DBT'), 'rb')
            bf = self.memofp.read(22)
            self.memo_block_size = struct.unpack('h',bf[20:22])[0]
        except Exception as err:
            print('Cannot open .DBT file!\n')
    
    def parseDBFInfo(self):
        try:
            self.file_size = len(self.buffer)
            self.end_char = self.buffer[-1]
            
            self.version_b = self.buffer[:1]
            self.version = bitstring.BitArray(self.version_b).bin
            
            y, = struct.unpack('<b', self.buffer[1:2]) # signed char
            m, = struct.unpack('<b', self.buffer[2:3])
            d, = struct.unpack('<b', self.buffer[3:4])
            self.dt_last_update = f'{d}/{m}/{1900+y}' # Προσθέτω 1900 ή 2000 ???
            
            self.nrt = struct.unpack('<i', self.buffer[4:8])[0] # int
            self.nbh = struct.unpack('<h', self.buffer[8:10])[0] # short
            self.nbr = struct.unpack('<h', self.buffer[10:12])[0] # short
            
            self.nrt_deleted = 0
        except Exception as err:
            print(str(err))

    def parseDBFMetadata(self):
        try:
            self.metadata = []
            pos = 32
            fn = 1
            while pos < (self.nbh - 1):
                fld = self.buffer[pos:pos+32]
                fld_n = fld[:11].decode(self.encoding).rstrip('\x00')
                fld_t = fld[11:12].decode('latin')
                fld_l, = struct.unpack('<B', fld[16:17])
                fld_d, = struct.unpack('<B', fld[17:18])
                
                if fld_t in ('M','B'): # Αν υπάρχει memo field, στα metadata σαν μέγεθος βάζω το μεγαλύτερο που θα βρω στο αρχείο .DBT και κρατάω το μέγεθος του πεδίου του .DBF στο self.memo_block_number_size
                    self.memo_field_exists = True
                    self.memo_block_number_size = fld_l
                    fld_l = 0
                self.metadata.append([fld_n,fld_t,fld_l,fld_d,0,fn])
                pos += 32
                fn += 1
        except Exception as err:
            print(str(err))

    def parseDBFData(self):
        try:
            self.data = []
            pos = self.nbh
            for rn in range(0,self.nrt):
                rec = self.buffer[pos:pos+self.nbr]
                deleted_flag = rec[0:1]
                if deleted_flag == b'*':
                    self.nrt_deleted += 1 # Άθροισε τις διαγραμμένες εγγραφές
                if deleted_flag == b' ': # Παρακάμπτω τις διαγραμμένες εγγραφές που δεν αρχίζουν με κενό αλλά με '*'
                    fld_index = 1
                    row_tuple = ()
                    for column in self.metadata:
                        """
                        http://www.alexnolan.net/software/dbf.htm
                        Autoincrement column, ακέραιος τεσσέρων bytes
                        Το πρόγραμμα DBFPlus.exe σε autoincrement αρχίζει και αριθμεί τις εγγραφές από 1, 2, 3 κλπ και δεν ξέρω τι ακριβώς κάνει.
                        Το buffer των τεσσάρων bytes αρχίζει πάντα από b'\x80' (Ο χαρακτήρας 128, του Euro €)
                        Το παραγόμενο αποτέλεσμα και με τους τρεις τρόπους είναι το ίδιο
                        """
                        match column[1]:
                            case '+': # Autoincrement
                                fb = rec[fld_index:fld_index+column[2]]
                                fb = bytes([fb[3],fb[2],fb[1],fb[1]]) # Για να πάρω αυτό που μου δίνει το DBFViewer Plus πρέπει να απαλείψω το \x80 και να γυρίσω τα bytes ανάποδα ???
                                fv, = struct.unpack('<i',fb)
                                #fv = int(fb[::-1].hex(), 16)
                                #fv = int.from_bytes(fb, byteorder='little', signed=False)
                                #print(fb, fv)
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case 'L': # Logical
                                fv, = rec[fld_index:fld_index+column[2]].decode('latin')
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case 'N': # Numeric
                                fv = rec[fld_index:fld_index+column[2]].decode('latin').strip('\x00').strip()
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case 'C': # Character
                                fv = rec[fld_index:fld_index+column[2]].decode(self.encoding).strip('\x00').strip()
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case 'D': # Date
                                fv_raw = rec[fld_index:fld_index+column[2]].decode('latin').strip('\x00').strip()
                                if fv_raw and len(fv_raw) == 8 and fv_raw.isdigit():
                                    y = int(fv_raw[0:4]); m = int(fv_raw[4:6]); d = int(fv_raw[6:8])
                                    fv = datetime.date(y, m, d)
                                else:
                                    fv = None
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case '@': # Timestamp
                                fb = rec[fld_index:fld_index+column[2]]
                                fv = datetime.date(int(fb[:4]), int(fb[4:6]), int(fb[6:8]))
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case 'I': # Long
                                fv, = struct.unpack('<i',rec[fld_index:fld_index+column[2]])
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case 'F': # Float
                                fb = rec[fld_index:fld_index+column[2]].strip().strip(b'*')
                                if fb:
                                    fv = float(fb)
                                    row_tuple += (fv,)
                                    fld_index += column[2]
                                else:
                                    fv = None
                            case 'O': # Double
                                fv, = struct.unpack('<d',rec[fld_index:fld_index+column[2]])
                                row_tuple += (fv,)
                                fld_index += column[2]
                            case c if c in ['M', 'B']: # Memo / Binary
                                mids = rec[fld_index:fld_index+self.memo_block_number_size].decode('latin').strip()
                                if mids != '':
                                    fv = self.readMemo(int(mids))
                                    if fv is not None:
                                        fv = fv.decode(self.encoding)
                                        fvs = len(fv)
                                        if fvs > self.metadata[column[5]-1][2]:  # Ενημερώνω το max memo size
                                            self.metadata[column[5]-1][2] = fvs
                                    else:
                                        fv = ''
                                else:
                                    fv = ''
                                row_tuple += (fv,)
                                fld_index += self.memo_block_number_size
                            case _:
                                print(f'Unknown data type: {column[1]}!')
                                break
                    
                    self.data.append(row_tuple) # Append μόνο αν δεν είναι deleted
                
                pos += self.nbr # Shift πάντα και σε διαγραμμένες εγγραφές
        except Exception as err:
            print(str(err))

    def readMemo(self, mid):
        if self.memofp is not None:
            self.memofp.seek(mid * self.memo_block_size)
            bf = self.memofp.read(8)
            if bf is not None and len(bf) == 8:
                ts = struct.unpack('<i', bf[4:8])[0]
                if ts > self.memo_biggest_size:
                    self.memo_biggest_size = ts
                if ts > 0:
                    self.memofp.seek(mid * self.memo_block_size)
                    bf = self.memofp.read(ts)[8:]
                else:
                    bf = None
            else:
                bf = None
            return bf