#pragma once

#include "cache.h"

#include "flat_page_base.h"
#include "flat_page_label.h"
#include "flat_row_nulls.h"
#include "util_basics.h"
#include "util_deref.h"

#include <ydb/library/actors/util/shared_data.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <util/generic/buffer.h>
#include <memory>

namespace NKikimr {
namespace NTable {
namespace NPage {


    struct TDataPageLayout {

        /*
                TRecord binary layout v1
            .-------------------------.
            | ERowOp and bit-flags    | header
            .---------.---------------.       -.
            | cell op | value OR offs | cell_1 |
            .---------.---------------.        |
            |       .    .    .       |        | fixed-size
            .---------.---------------.        |
            | cell op | value OR offs | cell_K |
            .-.------.'-.-------.-----.       -'
            | |      |  |       |     | var-size values
            '-'------'--'-------'-----'
        */

#pragma pack(push,1)

        struct TItem {
            TCellOp GetCellOp(bool key) const noexcept
            {
                if (key)
                    return Null ? ECellOp::Null : ECellOp::Set;
                return TCellOp::Decode(Flg);
            }

            union {
                bool Null;
                ui8 Flg;
            };
        } Y_PACKED;

        struct TVersion {
            ui64 Step_;
            ui64 TxId_;

            TRowVersion Get() const noexcept {
                return TRowVersion(Step_, TxId_);
            }

            void Set(const TRowVersion& v) noexcept {
                Step_ = v.Step;
                TxId_ = v.TxId;
            }
        } Y_PACKED;

        struct TDelta {
            ui64 TxId_;

            ui64 GetTxId() const noexcept {
                return TxId_;
            }

            void SetTxId(ui64 txId) noexcept {
                TxId_ = txId;
            }
        } Y_PACKED;

        struct TLocked {
            ELockMode LockMode_;
            ui64 LockTxId_;

            ELockMode GetLockMode() const noexcept {
                return LockMode_;
            }

            ui64 GetLockTxId() const noexcept {
                return LockTxId_;
            }

            void Set(ELockMode mode, ui64 txId) noexcept {
                LockMode_ = mode;
                LockTxId_ = txId;
            }
        } Y_PACKED;

        struct TRecord : public TDataPageRecord<TRecord, TItem> {
            ui8 Fields_;

            ERowOp GetRop() const noexcept {
                return ERowOp(Fields_ & 0x0F);
            }

            bool IsErased() const noexcept {
                return Fields_ & 0x80;
            }

            bool IsVersioned() const noexcept {
                return Fields_ & 0x40;
            }

            bool HasHistory() const noexcept {
                return Fields_ & 0x20;
            }

            bool IsLocked() const noexcept {
                return Fields_ & 0x10;
            }

            bool IsDelta() const noexcept {
                /* HasHistory without mvcc bits set */
                return (Fields_ & 0xE0) == 0x20;
            }

            void SetZero() noexcept {
                Fields_ = 0;
            }

            void SetFields(ERowOp rop, bool erased, bool versioned, bool delta, bool locked) noexcept {
                Fields_ = ui8(rop) | (locked ? 0x10 : 0) | (delta ? 0x20 : ((erased ? 0x80 : 0) | (versioned ? 0x40 : 0)));
            }

            void MarkHasHistory() noexcept {
                Fields_ |= 0x20;
            }

            // N.B. Get Min/Max version and GetDeltaTxId below read from the same record position
            // right after column entries. Up to caller to decide what kind of data is there

            TRowVersion GetMaxVersion(const TPartScheme::TGroupInfo& group) const {
                Y_DEBUG_ABORT_UNLESS(IsErased());
                return GetTail<TVersion>(group)->Get();
            }

            TRowVersion GetMinVersion(const TPartScheme::TGroupInfo& group) const {
                Y_DEBUG_ABORT_UNLESS(IsVersioned());
                auto* v = GetTail<TVersion>(group);
                if (IsErased()) {
                    ++v;
                }
                return v->Get();
            }

            ui64 GetDeltaTxId(const TPartScheme::TGroupInfo& group) const {
                Y_DEBUG_ABORT_UNLESS(IsDelta());
                return GetTail<TDelta>(group)->GetTxId();
            }

            static size_t GetLockInfoOffset(ui8 fields) {
                static constexpr size_t offset[] = {
                    /* 000xyyyy = 0x00 */ 0,
                    /* 001xyyyy = 0x20 */ sizeof(TDelta),
                    /* 010xyyyy = 0x40 */ sizeof(TVersion),
                    /* 011xyyyy = 0x60 */ sizeof(TVersion),
                    /* 100xyyyy = 0x80 */ sizeof(TVersion),
                    /* 101xyyyy = 0xa0 */ sizeof(TVersion),
                    /* 110xyyyy = 0xc0 */ sizeof(TVersion) * 2,
                    /* 111xyyyy = 0xd0 */ sizeof(TVersion) * 2,
                };
                return offset[fields >> 5];
            }

            std::tuple<ELockMode, ui64> GetLockInfo(const TPartScheme::TGroupInfo& group) const {
                Y_DEBUG_ABORT_UNLESS(IsLocked());
                auto* l = GetTail<TLocked>(group, GetLockInfoOffset(Fields_));
                return { l->GetLockMode(), l->GetLockTxId() };
            }

            const TRecord* GetAltRecord(size_t index) const {
                if (index == 0) {
                    return this;
                }
                const char* base = reinterpret_cast<const char*>(this);
                ui64 offset = ReadUnaligned<ui64>(base - sizeof(ui64) * index);
                if (offset != 0) {
                    return reinterpret_cast<const TRecord*>(base + offset);
                } else {
                    return nullptr;
                }
            }

        private:
            // returns pointer to the data right after all column entries
            template <class T>
            const T* GetTail(const TPartScheme::TGroupInfo& group, size_t off = 0) const {
                return TDeref<T>::At(Base(), group.FixedSize + off);
            }
        } Y_PACKED;

        struct TExtra {
            TRowId BaseRow;
        } Y_PACKED;

#pragma pack(pop)

        static_assert(sizeof(TItem) == 1, "Invalid TDataPage TItem size");
        static_assert(sizeof(TVersion) == 16, "Invalid TDataPage TVersion size");
        static_assert(sizeof(TDelta) == 8, "Invalid TDataPage TDelta size");
        static_assert(sizeof(TLocked) == 9, "Invalid TDataPage TLocked size");
        static_assert(sizeof(TRecord) == 1, "Invalid TDataPage TRecord size");
        static_assert(sizeof(TExtra) == 8, "Invalid TDataPage page extra chunk");

        enum class EStorageKind : ui8 {
            SharedData,
            BorrowedPage,
        };

        using TBlock = TBlockWithRecords<TRecord, EStorageKind>;

    public:
        using TIter = TBlock::TIterator;
    };

    template <class TInputRef>
    class TDataPageStorage : public TDataPageLayout {
    protected:
        TDataPageStorage() noexcept = default;

        TDataPageStorage(const TDataPageStorage& other) noexcept
            : Page(other.Page)
            , BaseRow_(other.BaseRow_)
        {
            AssignStorage(other);
        }

        TDataPageStorage(TDataPageStorage&& other) noexcept
            : Page(other.Page)
            , BaseRow_(other.BaseRow_)
        {
            AssignStorage(std::move(other));
        }

        TDataPageStorage& operator=(const TDataPageStorage& other) noexcept {
            if (this != &other) {
                ResetDecoded();
                AssignStorage(other);
                Page = other.Page;
                BaseRow_ = other.BaseRow_;
            }
            return *this;
        }

        TDataPageStorage& operator=(TDataPageStorage&& other) noexcept {
            if (this != &other) {
                ResetDecoded();
                AssignStorage(std::move(other));
                Page = other.Page;
                BaseRow_ = other.BaseRow_;
            }
            return *this;
        }

        ~TDataPageStorage() {
            if constexpr (!std::is_same_v<TInputRef, TCachePageRef>) {
                if (IsBorrowed()) {
                    std::destroy_at(&Storage_.InputRef);
                    return;
                }
            }
            std::destroy_at(&Storage_.Decoded);
        }

        const TSharedData& DecodedData() const noexcept {
            Y_DEBUG_ABORT_UNLESS(!IsBorrowed());
            return Storage_.Decoded;
        }

        TSharedData& ResetDecoded() noexcept {
            if constexpr (!std::is_same_v<TInputRef, TCachePageRef>) {
                if (IsBorrowed()) {
                    std::destroy_at(&Storage_.InputRef);
                    std::construct_at(&Storage_.Decoded);
                    Page.Metadata = EStorageKind::SharedData;
                    return Storage_.Decoded;
                }
            }
            Storage_.Decoded = {};
            return Storage_.Decoded;
        }

        void SetBorrowed(TInputRef ref) noexcept
            requires(!std::is_same_v<TInputRef, TCachePageRef>)
        {
            Y_DEBUG_ABORT_UNLESS(!IsBorrowed());
            std::destroy_at(&Storage_.Decoded);
            std::construct_at(&Storage_.InputRef, std::move(ref));
            Page.Metadata = EStorageKind::BorrowedPage;
        }

        TSharedData ShareData() const noexcept {
            if constexpr (!std::is_same_v<TInputRef, TCachePageRef>) {
                if (IsBorrowed()) {
                    return NSharedCache::SharePageBuffer(Storage_.InputRef);
                }
            }
            return Storage_.Decoded;
        }

    private:
        bool IsBorrowed() const noexcept {
            if constexpr (std::is_same_v<TInputRef, TCachePageRef>) {
                return false;
            } else {
                return Page.Metadata == EStorageKind::BorrowedPage;
            }
        }

        // The empty SharedData alternative is active on entry
        template <class TOther>
        void AssignStorage(TOther&& other) noexcept {
            if constexpr (!std::is_same_v<TInputRef, TCachePageRef>) {
                if (other.IsBorrowed()) {
                    std::destroy_at(&Storage_.Decoded);
                    std::construct_at(&Storage_.InputRef, std::forward<TOther>(other).Storage_.InputRef);
                    return;
                }
            }
            Storage_.Decoded = std::forward<TOther>(other).Storage_.Decoded;
        }

        union TStorage {
            TSharedData Decoded;
            TInputRef InputRef;

            TStorage() noexcept
                : Decoded()
            {}

            // TDataPageStorage destroys the member selected by Page.Metadata
            ~TStorage() {}
        } Storage_;

    protected:
        TBlock Page;
        TRowId BaseRow_ = Max<TRowId>();
    };

    template <class TInputRef>
    struct TDataPageImpl : public TDataPageStorage<TInputRef> {
    private:
        using ICodec = NBlockCodecs::ICodec;
        using TBase = TDataPageStorage<TInputRef>;
        using TBase::BaseRow_;
        using TBase::DecodedData;
        using TBase::Page;
        using TBase::ResetDecoded;
        using TBase::SetBorrowed;
        using TBase::ShareData;

    public:
        using TBlock = TDataPageLayout::TBlock;
        using TIter = TDataPageLayout::TIter;
        using TRecord = TDataPageLayout::TRecord;
        using TExtra = TDataPageLayout::TExtra;

        TDataPageImpl(const TSharedData* raw = nullptr)
        {
            Set(raw);
        }

        explicit TDataPageImpl(TInputRef&& ref) {
            Set(std::move(ref));
        }

        NPage::TLabel Label() const noexcept
        {
            return ReadUnaligned<NPage::TLabel>(Page.Base);
        }

        explicit operator bool() const noexcept
        {
            return Page.Base != nullptr;
        }

        const TBlock* operator->() const noexcept
        {
            return &Page;
        }

        TRowId BaseRow() const noexcept
        {
            return BaseRow_;
        }

        const char* data() const noexcept {
            return static_cast<const char*>(Page.Base);
        }

        TSharedData BuildSharedData() const noexcept {
            return ShareData();
        }

        TDataPageImpl& Set(const TSharedData* raw = nullptr)
        {
            return Parse(raw ? *raw : TSharedData());
        }

        TDataPageImpl& Set(TInputRef&& ref) {
            if constexpr (std::is_same_v<TInputRef, TCachePageRef>) {
                TSharedData raw = ref ? ref.BuildSharedData() : TSharedData();
                ref.Drop();
                return Parse(std::move(raw));
            } else {
                Parse(ref ? TArrayRef<const char>(ref.data(), ref.size()) : TArrayRef<const char>(), {});
                if (ref && !DecodedData()) {
                    SetBorrowed(ref);
                }
                return *this;
            }
        }

    private:
        TDataPageImpl& Parse(TSharedData raw) {
            const TArrayRef<const char> bytes(raw);
            return Parse(bytes, std::move(raw));
        }

        TDataPageImpl& Parse(TArrayRef<const char> raw, TSharedData owner) {
            TSharedData& Decoded = ResetDecoded();
            Page = { };

            if (raw) {
                const void* base = raw.data();
                auto data = NPage::TLabelWrapper().Read(raw, EPage::DataPage);

                Y_ENSURE(data.Version == 1, "Unknown EPage::DataPage version");

                if (data.Codec != ECodec::Plain) {
                    /* Compressed, should convert to regular page */

                    Y_ENSURE(data == ECodec::LZ4, "Only LZ4 encoding allowed");

                    static const ICodec* const Codec = NBlockCodecs::Codec("lz4fast");
                    auto size = Codec->DecompressedLength(data.Page);

                    // We expect original page had the same label size as a compressed page
                    size_t labelSize = reinterpret_cast<const char*>(data.Page.data()) - reinterpret_cast<const char*>(base);

                    Decoded = TSharedData::Uninitialized(labelSize + size);

                    size = Codec->Decompress(data.Page, Decoded.mutable_begin() + labelSize);

                    Decoded.TrimBack(labelSize + size);
                    ::memcpy(Decoded.mutable_begin(), base, labelSize);

                    base = Decoded.begin();
                    data.Page = { Decoded.begin() + labelSize, Decoded.end() };
                } else {
                    Decoded = std::move(owner);
                }

                auto *recordsHeader = TDeref<TRecordsHeader>::At(data.Page.data(), 0);
                auto count = recordsHeader->Count;

                BaseRow_ = TDeref<const TExtra>::At(recordsHeader + 1, 0)->BaseRow;
                Page.Base = base;
                auto offsetsOffset = data.Page.size() - count * sizeof(TPgSize);
                Page.Offsets = TDeref<const TRecordsEntry>::At(recordsHeader, offsetsOffset);
                Page.Count = count;
            } else {
                Decoded = {};
                BaseRow_ = Max<TRowId>();
            }

            return *this;
        }

    public:
        const TSharedData& GetData() const noexcept
            requires std::is_same_v<TInputRef, TCachePageRef>
        {
            return DecodedData();
        }

        TIter LookupKey(TCells key, const TPartScheme::TGroupInfo &group,
                        ESeek seek, const TKeyCellDefaults *keyDefaults) const
        {
            if (!key) {
                switch (seek) {
                    case ESeek::Lower:
                        return Page.Begin();
                    case ESeek::Exact:
                    case ESeek::Upper:
                        return Page.End();
                }
            }

            TIter it;

            const auto cmp = TCompare<TRecord>(group.ColsKeyData, *keyDefaults);
            switch (seek) {
                case ESeek::Exact:
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    // key <= it->Key
                    if (it && cmp(key, *it)) {
                        it = Page.End();
                    }
                    return it;
                case ESeek::Lower: {
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                }
                case ESeek::Upper: {
                    it = std::upper_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                }
            }

            return it;
        }

        TIter LookupKeyReverse(TCells key, const TPartScheme::TGroupInfo &group,
                        ESeek seek, const TKeyCellDefaults *keyDefaults) const
        {
            if (!key) {
                switch (seek) {
                    case ESeek::Lower:
                        return --Page.End();
                    case ESeek::Exact:
                    case ESeek::Upper:
                        return Page.End();
                }
            }

            TIter it;

            const auto cmp = TCompare<TRecord>(group.ColsKeyData, *keyDefaults);
            switch (seek) {
                case ESeek::Exact:
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    // key <= it->Key
                    if (it && cmp(key, *it)) {
                        it = Page.End();
                    }
                    return it;
                case ESeek::Lower:
                    it = std::upper_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
                case ESeek::Upper:
                    it = std::lower_bound(Page.Begin(), Page.End(), key, cmp);
                    break;
            }

            if (it.Off() == 0) {
                it = Page.End();
            } else {
                --it;
            }

            return it;
        }
    };

    using TDataPage = TDataPageImpl<TCachePageRef>;
    using TBorrowedDataPage = TDataPageImpl<TCachePageConstRef>;
}
}
}
