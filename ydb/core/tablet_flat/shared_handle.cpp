#include "shared_cache.h"
#include "shared_handle.h"

namespace NKikimr::NSharedCache {
namespace {

    class TCachePageHandle final : public TSharedPageHandle {
    public:
        explicit TCachePageHandle(TSharedCachePageRef&& page)
            : Page_(std::move(page))
        {
            Initialize(Page_.ShareData());
        }

        bool IsSticky() const noexcept override {
            return Page_.IsSticky();
        }

    private:
        TSharedCachePageRef Page_;
    };

} // anonymous namespace

TSharedPageRef MakeSharedPageRef(TSharedCachePageRef&& page) {
    Y_DEBUG_ABORT_UNLESS(page);
    const NTable::NPage::EPage type = page.GetType();
    return TSharedPageRef::MakeUsed(new TCachePageHandle(std::move(page)), nullptr, type);
}

} // namespace NKikimr::NSharedCache
