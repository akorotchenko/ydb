#pragma once

#include "flat_page_iface.h"
#include "flat_sausage_solid.h"
#include "flat_table_stats.h"
#include "flat_row_eggs.h"
#include "util_basics.h"
#include "shared_cache.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/string.h>
#include <util/system/types.h>
#include <new>

namespace NKikimr {
namespace NTable {

    class TColdPart;
    class TPart;
    class TMemTable;

    using TPageId = NPage::TPageId;
    using TPageOffset = NPage::TPageOffset;
    using TPageLocation = NPage::TPageLocation;
    using EPage = NPage::EPage;

    struct ISaver {
        using TGlobId = NPageCollection::TGlobId;

        virtual void Save(TSharedData raw, NPage::TGroupId groupId) = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, const TGlobId &glob) = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, TArrayRef<const char> blob) = 0;
    };

    class IPageWriter {
    public:
        using EPage = NPage::EPage;
        using TPageId = NPage::TPageId;

        virtual ~IPageWriter() = default;
        virtual TPageLocation Write(TSharedData page, EPage type, ui32 group) = 0;
        virtual TPageId WriteOuter(TSharedData) = 0;
        virtual void WriteInplace(TPageId page, TArrayRef<const char> body) = 0;
        virtual NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) = 0;
        virtual void Finish(TString overlay) = 0;

        virtual ui32 GetLastWrittenPageId(ui32 group) const noexcept = 0;
    };

    struct IPages {
        using ELargeObj = NTable::ELargeObj;
        using TPart = NTable::TPart;
        using TMemTable = NTable::TMemTable;
        using TPageId = NPage::TPageId;
        using TPageLocation = NPage::TPageLocation;
        using TGroupId = NPage::TGroupId;

        virtual ~IPages() = default;

        struct TResult : private TMoveOnly {
            TResult(bool need, const TSharedData* data)
                : Need(need)
                , Kind_(EKind::Borrowed)
                , BorrowedData_(data)
            {}

            TResult(bool need, TSharedData&& data)
                : Need(need)
                , Kind_(EKind::Data)
            {
                new (&Data_) TSharedData(std::move(data));
            }

            TResult(bool need, TSharedPageRef&& page)
                : Need(need)
                , Kind_(EKind::Page)
            {
                new (&Page_) TSharedPageRef(std::move(page));
            }

            TResult(TResult&& other) noexcept
                : Need(other.Need)
                , Kind_(other.Kind_)
            {
                MovePayload(other);
            }

            TResult& operator=(TResult&& other) noexcept {
                if (this != &other) {
                    DestroyPayload();
                    Need = other.Need;
                    Kind_ = other.Kind_;
                    MovePayload(other);
                }
                return *this;
            }

            ~TResult() {
                DestroyPayload();
            }

            explicit operator bool() const noexcept {
                switch (Kind_) {
                    case EKind::Borrowed:
                        return bool(BorrowedData_);
                    case EKind::Data:
                        return bool(Data_);
                    case EKind::Page:
                        return bool(Page_);
                }
                Y_UNREACHABLE();
            }

            TArrayRef<const char> Data() const noexcept {
                switch (Kind_) {
                    case EKind::Borrowed:
                        return BorrowedData_ ? TArrayRef<const char>(*BorrowedData_) : TArrayRef<const char>();
                    case EKind::Data:
                        return Data_;
                    case EKind::Page:
                        return Page_ ? TArrayRef<const char>(Page_.data(), Page_.size()) : TArrayRef<const char>();
                }
                Y_UNREACHABLE();
            }

            bool HasOwner() const noexcept {
                return Kind_ != EKind::Borrowed;
            }

            bool Need;

        private:
            enum class EKind : ui8 {
                Borrowed,
                Data,
                Page,
            };

            void MovePayload(TResult& other) noexcept {
                switch (Kind_) {
                    case EKind::Borrowed:
                        BorrowedData_ = std::exchange(other.BorrowedData_, nullptr);
                        break;
                    case EKind::Data:
                        new (&Data_) TSharedData(std::move(other.Data_));
                        break;
                    case EKind::Page:
                        new (&Page_) TSharedPageRef(std::move(other.Page_));
                        break;
                }
            }

            void DestroyPayload() noexcept {
                switch (Kind_) {
                    case EKind::Borrowed:
                        break;
                    case EKind::Data:
                        Data_.~TSharedData();
                        break;
                    case EKind::Page:
                        Page_.~TSharedPageRef();
                        break;
                }
            }

            EKind Kind_;

            union {
                const TSharedData* BorrowedData_;
                TSharedData Data_;
                TSharedPageRef Page_;
            };
        };

        static_assert(sizeof(TResult) == 24);

        virtual TResult Locate(const TMemTable*, ui64 ref, ui32 tag) = 0;
        virtual TResult Locate(const TPart*, ui64 ref, ELargeObj lob) = 0;
        virtual TSharedPageRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) = 0;

        /**
         * Hook for cleaning up env on DB.RollbackChanges()
         */
        virtual void OnRollbackChanges() {
            // nothing by default
        }
    };

}
}
